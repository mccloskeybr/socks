use crate::chunk;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::*;
use crate::CACHE_SIZE;

// TODO: for concurrency support, could expose the entry directly, along with
// some locking mechanism (per-chunk).
#[derive(Default, Clone)]
struct CacheEntry {
    node: NodeProto,
    counter: usize,
}

#[derive(Default, Clone)]
pub(crate) struct Cache {
    entries: [CacheEntry; CACHE_SIZE],
    next_counter: usize,
}

fn next_counter(cache: &mut Cache) -> usize {
    let counter = cache.next_counter;
    cache.next_counter += 1;
    counter
}

// Finds the index associated with offset, else the index of the least
// recently used element. Returns the index, and true iff the requested index
// was found.
fn find_idx(cache: &Cache, offset: u32) -> (usize, bool) {
    let mut lru_idx: usize = 0;
    for idx in 0..cache.entries.len() {
        if cache.entries[idx].node.offset == offset {
            return (idx, true);
        }
        if cache.entries[idx].counter < cache.entries[lru_idx].counter {
            lru_idx = idx;
        }
    }
    return (lru_idx, false);
}

// TODO: currently copies chunk data, could use Arc if more efficient?
pub fn read<F: Filelike>(table: &mut Table<F>, offset: u32) -> Result<NodeProto, Error> {
    let cache = &mut table.cache;
    let (idx, in_cache) = find_idx(cache, offset);
    if !in_cache {
        cache.entries[idx].node = chunk::read_chunk_at::<F, NodeProto>(&mut table.file, offset)?;
    }
    cache.entries[idx].counter = next_counter(cache);
    Ok(cache.entries[idx].node.clone())
}

// TODO: currently copies data 2x. copy + move possible?
pub fn write<F: Filelike>(table: &mut Table<F>, node: &NodeProto) -> Result<(), Error> {
    let cache = &mut table.cache;
    let (idx, _) = find_idx(cache, node.offset);
    cache.entries[idx].node = node.clone();
    cache.entries[idx].counter = next_counter(cache);
    chunk::write_chunk_at::<F, NodeProto>(
        &mut table.file,
        cache.entries[idx].node.clone(),
        cache.entries[idx].node.offset,
    )
}
