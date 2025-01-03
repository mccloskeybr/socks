use crate::buffer::Buffer;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::*;
use crate::{BUFFER_POOL_SHARD_COUNT, BUFFER_POOL_SHARD_SIZE};
use std::cell::OnceCell;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

// Individual LRU cache entries. Buffers are stored behind a lock to ensure
// buffers cannot be written to while read.
#[derive(Debug)]
struct CacheEntry<F: Filelike> {
    data: OnceCell<Arc<RwLock<Buffer<F, NodeProto>>>>,
    table_id: u32,
    offset: u32,
    left: CacheEntryPtr<F>,
    right: CacheEntryPtr<F>,
}

#[derive(Debug)]
struct CacheEntryBox<F: Filelike>(*mut CacheEntry<F>);

impl<F: Filelike> CacheEntryBox<F> {
    fn new(entry: CacheEntry<F>) -> Self {
        Self(Box::into_raw(Box::new(entry)))
    }

    fn as_ptr(&self) -> CacheEntryPtr<F> {
        CacheEntryPtr(self.0)
    }
}

impl<F: Filelike> Drop for CacheEntryBox<F> {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.0 as *mut CacheEntry<F>));
        }
    }
}

#[derive(Debug)]
struct CacheEntryPtr<F: Filelike>(*mut CacheEntry<F>);

unsafe impl<F: Filelike> Send for CacheEntryPtr<F> {}

impl<F: Filelike> Deref for CacheEntryPtr<F> {
    type Target = CacheEntry<F>;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}

impl<F: Filelike> DerefMut for CacheEntryPtr<F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.0 }
    }
}

impl<F: Filelike> Clone for CacheEntryPtr<F> {
    fn clone(&self) -> Self {
        CacheEntryPtr(self.0)
    }
}

// LRU cache implementation. Intended to be accessed behind a Mutex.
// Recency is tracked through a doubly-linked list (tracked by sentinel).
// Performant data retrieval is supported using the HashMap.
struct Cache<F: Filelike> {
    sentinel: CacheEntryBox<F>,
    map: HashMap<(u32, u32), CacheEntryBox<F>>,
}

unsafe impl<F: Filelike> Send for Cache<F> {}

impl<F: Filelike> Cache<F> {
    fn new() -> Self {
        let sentinel = CacheEntryBox::new(CacheEntry {
            data: OnceCell::new(),
            table_id: 0,
            offset: 0,
            left: CacheEntryPtr(std::ptr::null_mut()),
            right: CacheEntryPtr(std::ptr::null_mut()),
        });
        let mut sentinel_ptr = sentinel.as_ptr();
        sentinel_ptr.left = sentinel_ptr.clone();
        sentinel_ptr.right = sentinel_ptr.clone();
        Self {
            sentinel: sentinel,
            map: HashMap::new(),
        }
    }

    // Evict the least recently used item from the cache.
    // NOTE: Expects the cache to have at least one element!
    async fn evict(&mut self) -> Result<(), Error> {
        debug_assert!(self.map.len() > 0);
        let mut lru = self.sentinel.as_ptr().left.clone();
        lru.left.right = lru.right.clone();
        lru.right.left = lru.left.clone();

        // NOTE: since this shard must be locked to retrieve the buffer lock, once
        // this exclusive lock request succeeds we know there are no races on the
        // evicted buffer. if there is a pending request to read the buffer we're
        // about to evict, we will simply re-read it in, after any dirty data has
        // been committed and the locks are released.
        let buffer = lru.data.get().unwrap().write().await;
        buffer.write_to_file().await?;

        self.map.remove(&(lru.table_id, lru.offset));

        Ok(())
    }

    // Marks the provided node as "most" recently used by moving it to the
    // front of the list.
    fn promote(&mut self, entry_ptr: &mut CacheEntryPtr<F>) {
        entry_ptr.left = self.sentinel.as_ptr().clone();
        entry_ptr.right = self.sentinel.as_ptr().right.clone();
        entry_ptr.left.right = entry_ptr.clone();
        entry_ptr.right.left = entry_ptr.clone();
    }

    // Inserts the buffer into the cache, marks it as most recently used.
    // NOTE: Expects the buffer to not already be present!
    async fn insert(
        &mut self,
        table_id: u32,
        offset: u32,
        buffer: Buffer<F, NodeProto>,
    ) -> Result<Arc<RwLock<Buffer<F, NodeProto>>>, Error> {
        debug_assert!(self.get(table_id, offset).await.is_none());
        if self.map.len() >= BUFFER_POOL_SHARD_SIZE {
            self.evict().await?;
        }
        let entry_box = CacheEntryBox::new(CacheEntry {
            data: OnceCell::from(Arc::new(RwLock::new(buffer))),
            table_id: table_id,
            offset: offset,
            left: CacheEntryPtr(std::ptr::null_mut()),
            right: CacheEntryPtr(std::ptr::null_mut()),
        });
        let mut entry_ptr = entry_box.as_ptr();
        self.map.insert((table_id, offset), entry_box);
        self.promote(&mut entry_ptr);
        Ok(entry_ptr.deref().data.get().unwrap().clone())
    }

    // Gets the buffer associated with the given table and offset.
    // Marks it as most recently used before returning.
    async fn get(
        &mut self,
        table_id: u32,
        offset: u32,
    ) -> Option<Arc<RwLock<Buffer<F, NodeProto>>>> {
        match self.map.get(&(table_id, offset)) {
            Some(entry_box) => {
                let mut entry_ptr = entry_box.as_ptr();
                // remove entry_ptr from the recency list.
                entry_ptr.left.right = entry_ptr.right.clone();
                entry_ptr.right.left = entry_ptr.left.clone();
                self.promote(&mut entry_ptr);
                return Some(entry_ptr.data.get().unwrap().clone());
            }
            None => {
                return None;
            }
        }
    }

    // Empties all buffers in the cache.
    // This forces all dirty / in-flight buffers to commit any changes to disk.
    #[cfg(test)]
    async fn flush(&mut self) -> Result<(), Error> {
        while self.map.len() > 0 {
            self.evict().await?;
        }
        Ok(())
    }
}

// Manages all in-memory buffers (B+ node buffers specifically). Intended to be
// shared across threads. Internally represented as an LRU cache, keyed on
// table id + offset. Sharded for more efficient concurrent access.
pub(crate) struct BufferPool<F: Filelike> {
    shards: Vec<Mutex<Cache<F>>>,
}

impl<F: Filelike> BufferPool<F> {
    // Finds what shard the given key is associated with.
    fn shard_idx(table_id: u32, offset: u32) -> usize {
        let cantor = (table_id + offset) * (table_id + offset + 1) / 2 + table_id;
        cantor as usize % BUFFER_POOL_SHARD_COUNT
    }

    pub(crate) fn new() -> Self {
        let mut shards: Vec<Mutex<Cache<F>>> = Vec::with_capacity(BUFFER_POOL_SHARD_COUNT);
        for _ in 0..BUFFER_POOL_SHARD_COUNT {
            shards.push(Mutex::new(Cache::new()));
        }
        Self { shards: shards }
    }

    // Claims the next offset for the given table and creates an empty buffer
    // at that location.
    pub(crate) async fn new_next_for_table(
        &self,
        table: &Table<F>,
    ) -> Result<Arc<RwLock<Buffer<F, NodeProto>>>, Error> {
        let buffer = Buffer::new_next_for_table(table).await;
        let mut shard = self.shards[Self::shard_idx(table.id, buffer.offset)]
            .lock()
            .await;
        shard.insert(table.id, buffer.offset, buffer).await
    }

    // Retrieves / reads the buffer on the given table at the given index.
    pub(crate) async fn read_from_table(
        &self,
        table: &Table<F>,
        offset: u32,
    ) -> Result<Arc<RwLock<Buffer<F, NodeProto>>>, Error> {
        let mut shard = self.shards[Self::shard_idx(table.id, offset)].lock().await;
        match shard.get(table.id, offset).await {
            Some(buffer) => return Ok(buffer),
            None => {
                let buffer = Buffer::read_from_table(table, offset).await?;
                return Ok(shard.insert(table.id, buffer.offset, buffer).await?);
            }
        }
    }

    // Forces all dirty / in-flight buffers to commit any changes to disk.
    #[cfg(test)]
    pub(crate) async fn flush(&self) -> Result<(), Error> {
        for shard in &self.shards {
            let mut shard = shard.lock().await;
            shard.flush().await?;
        }
        Ok(())
    }
}
