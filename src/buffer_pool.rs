use crate::buffer::Buffer;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::*;
use crate::{BUFFER_POOL_SHARD_COUNT, BUFFER_POOL_SHARD_SIZE};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::Mutex;

struct CacheEntry<F: Filelike> {
    data: Arc<Mutex<Buffer<F, NodeProto>>>,
    table_id: u32,
    offset: u32,
    left: CacheEntryPtr<F>,
    right: CacheEntryPtr<F>,
}

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
        let sentinel = CacheEntryBox::new(CacheEntry {
            data: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
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
        let lru = lru.deref_mut();
        lru.left.right = lru.right.clone();
        lru.right.left = lru.left.clone();

        // NOTE: since this shard must be locked to retrieve the buffer lock, once
        // this lock succeeds we know there are no outstanding threads using it.
        let mut buffer = lru.data.lock().await;
        if buffer.is_dirty {
            buffer.write_to_table().await?;
        }

        self.map.remove(&(lru.table_id, lru.offset));

        Ok(())
    }

    async fn insert(
        &mut self,
        table_id: u32,
        offset: u32,
        buffer: Buffer<F, NodeProto>,
    ) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
        debug_assert!(self.get(table_id, offset).await.is_none());
        if self.map.len() >= BUFFER_POOL_SHARD_SIZE {
            self.evict().await?;
        }
        let entry_box = CacheEntryBox::new(CacheEntry {
            data: Arc::new(Mutex::new(buffer)),
            table_id: table_id,
            offset: offset,
            left: self.sentinel.as_ptr().clone(),
            right: self.sentinel.as_ptr().right.clone(),
        });
        let entry_ptr = entry_box.as_ptr();
        self.map.insert((table_id, offset), entry_box);
        self.sentinel.as_ptr().right.left = entry_ptr.clone();
        self.sentinel.as_ptr().right = entry_ptr.clone();
        Ok(entry_ptr.deref().data.clone())
    }

    async fn get(
        &mut self,
        table_id: u32,
        offset: u32,
    ) -> Option<Arc<Mutex<Buffer<F, NodeProto>>>> {
        match self.map.get(&(table_id, offset)) {
            Some(entry_box) => {
                let mut entry_ptr = entry_box.as_ptr();
                entry_ptr.left.right = entry_ptr.right.clone();
                entry_ptr.right.left = entry_ptr.left.clone();
                entry_ptr.right = self.sentinel.as_ptr().right.clone();
                entry_ptr.left = self.sentinel.as_ptr().clone();
                self.sentinel.as_ptr().right = entry_ptr.clone();
                return Some(entry_ptr.data.clone());
            }
            None => {
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
        Self {
            shards: Vec::with_capacity(BUFFER_POOL_SHARD_COUNT),
        }
    }

    pub(crate) async fn new_for_table(
        &mut self,
        table: &mut Table<F>,
    ) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
        let buffer = Buffer::new_for_table(table);
        let shard = &mut *self.shards[Self::shard_idx(buffer.offset)].lock().await;
        shard.insert(table.metadata.id, buffer.offset, buffer).await
    }

    pub(crate) async fn read_from_table(
        &mut self,
        table: &mut Table<F>,
        offset: u32,
    ) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
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
}
