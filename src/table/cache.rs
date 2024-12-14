use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::table::chunk;
use crate::table::table::*;
use crate::CACHE_SIZE;
use std::io::{Read, Seek, Write};

// TODO: for concurrency support, could expose the entry directly, along with
// some locking mechanism (per-chunk).
#[derive(Default)]
struct CacheEntry {
    chunk: ChunkProto,
    counter: usize,
}

#[derive(Default)]
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
        if cache.entries[idx].chunk.node().offset == offset {
            return (idx, true);
        }
        if cache.entries[idx].counter < cache.entries[lru_idx].counter {
            lru_idx = idx;
        }
    }
    return (lru_idx, false);
}

// TODO: currently copies chunk data, could use Arc if more efficient?
pub fn read<F: Read + Write + Seek>(
    table: &mut Table<F>,
    offset: u32,
) -> Result<ChunkProto, Error> {
    let cache = &mut table.cache;
    let (idx, in_cache) = find_idx(cache, offset);
    if !in_cache {
        cache.entries[idx].chunk =
            chunk::read_chunk_at::<F>(&table.db_config.file, &mut table.file, offset)?;
    }
    cache.entries[idx].counter = next_counter(cache);
    Ok(cache.entries[idx].chunk.clone())
}

// TODO: currently copies data 2x. copy + move possible?
pub fn write<F: Read + Write + Seek>(
    table: &mut Table<F>,
    chunk: &ChunkProto,
) -> Result<(), Error> {
    let cache = &mut table.cache;
    let (idx, _) = find_idx(cache, chunk.node().offset);
    cache.entries[idx].chunk = chunk.clone();
    cache.entries[idx].counter = next_counter(cache);
    chunk::write_chunk_at::<F>(
        &table.db_config.file,
        &mut table.file,
        cache.entries[idx].chunk.clone(),
        cache.entries[idx].chunk.node().offset,
    )
}
