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
use tokio::sync::Mutex;

#[derive(Debug)]
struct CacheEntry<F: Filelike> {
    data: OnceCell<Arc<Mutex<Buffer<F, NodeProto>>>>,
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

struct Cache<F: Filelike> {
    // Circular doubly-linked list of decreasing priority.
    sentinel: CacheEntryBox<F>,
    map: HashMap<(u32, u32), CacheEntryBox<F>>,
}

impl<F: Filelike> Cache<F> {
    fn new() -> Self {
        // NOTE: sentinel is never accessed directly.
        let sentinel = CacheEntryBox::new(CacheEntry {
            data: OnceCell::new(),
            table_id: 0,
            offset: 0,
            left: CacheEntryPtr(std::ptr::null_mut()),
            right: CacheEntryPtr(std::ptr::null_mut()),
        });
        let mut sentinel_ptr = sentinel.as_ptr();
        sentinel_ptr.deref_mut().left = sentinel_ptr.clone();
        sentinel_ptr.deref_mut().right = sentinel_ptr.clone();
        Self {
            sentinel: sentinel,
            map: HashMap::new(),
        }
    }

    async fn evict(&mut self) -> Result<(), Error> {
        let mut lru = self.sentinel.as_ptr().left.clone();
        debug_assert!(lru.0 != self.sentinel.as_ptr().0);
        let lru = lru.deref_mut();
        dbg!(&lru);
        lru.left.right = lru.right.clone();
        lru.right.left = lru.left.clone();

        // NOTE: since this shard must be locked to retrieve the buffer lock, once
        // this lock succeeds we know there are no outstanding threads using it.
        let mut buffer = lru.data.get_mut().unwrap().lock().await;
        if buffer.is_dirty {
            buffer.write_to_table().await?;
        }

        self.map.remove(&(lru.table_id, lru.offset));

        Ok(())
    }

    #[cfg(test)]
    async fn flush(&mut self) -> Result<(), Error> {
        log::trace!("flush shard {}", self.map.len());
        while self.map.len() > 0 {
            log::trace!("flush elem");
            self.evict().await?;
        }
        Ok(())
    }

    async fn insert(
        &mut self,
        table_id: u32,
        offset: u32,
        buffer: Buffer<F, NodeProto>,
    ) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
        log::trace!("inserting");
        debug_assert!(self.get(table_id, offset).await.is_none());
        if self.map.len() >= BUFFER_POOL_SHARD_SIZE {
            log::trace!("evicting");
            self.evict().await?;
        }
        let data = OnceCell::new();
        data.set(Arc::new(Mutex::new(buffer))).unwrap();
        let entry_box = CacheEntryBox::new(CacheEntry {
            data: data,
            table_id: table_id,
            offset: offset,
            left: CacheEntryPtr(std::ptr::null_mut()),
            right: CacheEntryPtr(std::ptr::null_mut()),
        });
        let mut entry_ptr = entry_box.as_ptr();
        self.map.insert((table_id, offset), entry_box);
        log::trace!("items in map: {}", self.map.len());
        entry_ptr.left = self.sentinel.as_ptr().clone();
        entry_ptr.right = self.sentinel.as_ptr().right.clone();
        entry_ptr.left.right = entry_ptr.clone();
        entry_ptr.right.left = entry_ptr.clone();
        Ok(entry_ptr.deref().data.get().unwrap().clone())
    }

    async fn get(
        &mut self,
        table_id: u32,
        offset: u32,
    ) -> Option<Arc<Mutex<Buffer<F, NodeProto>>>> {
        log::trace!("retrieving...");
        match self.map.get(&(table_id, offset)) {
            Some(entry_box) => {
                log::trace!("found!");
                let mut entry_ptr = entry_box.as_ptr();
                entry_ptr.left.right = entry_ptr.right.clone();
                entry_ptr.right.left = entry_ptr.left.clone();
                entry_ptr.left = self.sentinel.as_ptr().clone();
                entry_ptr.right = self.sentinel.as_ptr().right.clone();
                entry_ptr.left.right = entry_ptr.clone();
                entry_ptr.right.left = entry_ptr.clone();
                return Some(entry_ptr.data.get().unwrap().clone());
            }
            None => {
                log::trace!("not found!");
                return None;
            }
        }
    }
}

pub(crate) struct BufferPool<F: Filelike> {
    shards: Vec<Mutex<Cache<F>>>,
}

impl<F: Filelike> BufferPool<F> {
    fn shard_idx(offset: u32) -> usize {
        (offset as usize) % BUFFER_POOL_SHARD_COUNT
    }

    pub(crate) fn new() -> Self {
        let mut shards: Vec<Mutex<Cache<F>>> = Vec::with_capacity(BUFFER_POOL_SHARD_COUNT);
        for _ in 0..BUFFER_POOL_SHARD_COUNT {
            shards.push(Mutex::new(Cache::new()));
        }
        Self { shards: shards }
    }

    pub(crate) async fn new_for_table(
        &mut self,
        table: &mut Table<F>,
    ) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
        let buffer = Buffer::new_for_table(table).await;
        log::trace!("new_for_table {}", buffer.offset);
        let shard = &mut *self.shards[Self::shard_idx(buffer.offset)].lock().await;
        shard.insert(table.metadata.id, buffer.offset, buffer).await
    }

    pub(crate) async fn read_from_table(
        &mut self,
        table: &mut Table<F>,
        offset: u32,
    ) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
        log::trace!("read_from_table: {}", offset);
        let shard = &mut *self.shards[Self::shard_idx(offset)].lock().await;
        match shard.get(table.metadata.id, offset).await {
            Some(buffer) => return Ok(buffer),
            None => {
                let buffer = Buffer::read_from_table(table, offset).await?;
                return Ok(shard
                    .insert(table.metadata.id, buffer.offset, buffer)
                    .await?);
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        log::trace!("flush pool");
        for shard in &mut self.shards {
            let mut shard = shard.lock().await;
            shard.flush().await?;
        }
        Ok(())
    }
}
