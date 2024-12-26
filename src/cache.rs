use crate::chunk;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::*;
use crate::{CACHE_SHARD_COUNT, CACHE_SHARD_SIZE};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

#[derive(Default)]
struct CacheEntry {
    node: NodeProto,
    table_id: u32,
    counter: usize,
}

// LRU cache is sharded to offer basic speedups wrt. thread contention.
// TODO: investigate other performance boosts if relevant.
#[derive(Default)]
struct CacheShard {
    entries: [CacheEntry; CACHE_SHARD_SIZE],
}

// TODO: write to disk only on eviction.
#[derive(Default)]
pub(crate) struct Cache {
    shards: [Mutex<CacheShard>; CACHE_SHARD_COUNT],
    lamport_clock: AtomicUsize,
}

// Finds the shard the given offset should be present in.
fn find_shard_idx(offset: u32) -> usize {
    (offset as usize) % CACHE_SHARD_COUNT
}

// Finds the index associated with offset, else the index of the least
// recently used element. Returns the index, and true iff the requested index
// was found.
fn find_entry_idx<F: Filelike>(shard: &CacheShard, table: &Table<F>, offset: u32) -> (usize, bool) {
    let mut lru_idx: usize = 0;
    for idx in 0..shard.entries.len() {
        let entry = &shard.entries[idx];
        if entry.table_id == table.metadata.id && entry.node.offset == offset {
            return (idx, true);
        }
        if entry.counter < shard.entries[lru_idx].counter {
            lru_idx = idx;
        }
    }
    return (lru_idx, false);
}

impl Cache {
    pub(crate) async fn read<F: Filelike>(
        &mut self,
        table: &mut Table<F>,
        offset: u32,
    ) -> Result<NodeProto, Error> {
        let shard = &mut *self.shards[find_shard_idx(offset)].lock().await;
        let (idx, in_cache) = find_entry_idx(shard, table, offset);
        if !in_cache {
            shard.entries[idx].node = chunk::read_chunk_at(&mut table.file, offset).await?;
        }
        shard.entries[idx].counter = self.lamport_clock.fetch_add(1, Ordering::Relaxed);
        Ok(shard.entries[idx].node.clone())
    }

    pub(crate) async fn write<F: Filelike>(
        &mut self,
        table: &mut Table<F>,
        node: &NodeProto,
    ) -> Result<(), Error> {
        let shard = &mut *self.shards[find_shard_idx(node.offset)].lock().await;
        let (idx, _) = find_entry_idx(shard, table, node.offset);
        shard.entries[idx].node = node.clone();
        shard.entries[idx].counter = self.lamport_clock.fetch_add(1, Ordering::Relaxed);
        chunk::write_chunk_at(
            &mut table.file,
            shard.entries[idx].node.clone(),
            shard.entries[idx].node.offset,
        )
        .await
    }
}
