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
    table_id: u32,
    counter: usize,
}

#[derive(Default, Clone)]
pub(crate) struct Cache {
    entries: [CacheEntry; CACHE_SIZE],
    next_counter: usize,
}

impl Cache {
    fn next_counter(&mut self) -> usize {
        let counter = self.next_counter;
        self.next_counter += 1;
        counter
    }

    // Finds the index associated with offset, else the index of the least
    // recently used element. Returns the index, and true iff the requested index
    // was found.
    fn find_idx<F: Filelike>(&self, table: &Table<F>, offset: u32) -> (usize, bool) {
        let mut lru_idx: usize = 0;
        for idx in 0..self.entries.len() {
            if self.entries[idx].table_id == table.metadata.id
                && self.entries[idx].node.offset == offset
            {
                return (idx, true);
            }
            if self.entries[idx].counter < self.entries[lru_idx].counter {
                lru_idx = idx;
            }
        }
        return (lru_idx, false);
    }

    pub(crate) fn read<F: Filelike>(
        &mut self,
        table: &mut Table<F>,
        offset: u32,
    ) -> Result<NodeProto, Error> {
        let (idx, in_cache) = self.find_idx(table, offset);
        if !in_cache {
            self.entries[idx].node = chunk::read_chunk_at(&mut table.file, offset)?;
        }
        self.entries[idx].counter = self.next_counter();
        Ok(self.entries[idx].node.clone())
    }

    pub(crate) fn write<F: Filelike>(
        &mut self,
        table: &mut Table<F>,
        node: &NodeProto,
    ) -> Result<(), Error> {
        let (idx, _) = self.find_idx(table, node.offset);
        self.entries[idx].node = node.clone();
        self.entries[idx].counter = self.next_counter();
        chunk::write_chunk_at(
            &mut table.file,
            self.entries[idx].node.clone(),
            self.entries[idx].node.offset,
        )
    }
}
