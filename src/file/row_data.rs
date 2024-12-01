#[cfg(test)]
#[path = "./row_data_test.rs"]
mod test;

use std::io::{Read, Write, Seek};
use crate::index::*;
use crate::error::*;
use crate::file::*;
use crate::protos::generated::chunk::*;

pub fn find_chunk<F: Read + Write + Seek>(index: &mut Index<F>, id: u32) -> Result<ChunkProto, Error> {
    let offset = directory::find_chunk_offset(index, id)?;
    index.num_chunk_reads += 1;
    chunk::read_chunk_at::<F>(&mut index.file, offset)
}

pub fn commit_chunk<F: Read + Write + Seek>(index: &mut Index<F>, chunk: &ChunkProto) -> Result<(), Error> {
    assert!(chunk.has_data());
    log::trace!("Committing chunk: {}.", chunk.data().id);
    match directory::find_chunk_offset(index, chunk.data().id) {
        Ok(chunk_offset) => {
            chunk::write_chunk_at::<F>(&mut index.file, chunk, chunk_offset)?;
            index.num_chunk_writes += 1;
        }
        Err(Error::NotFound(..)) => {
            let offset = index.metadata.next_chunk_offset;
            index.metadata.next_chunk_offset += 1;
            chunk::write_chunk_at::<F>(&mut index.file, chunk, offset)?;
            directory::create_directory_entry(index, chunk.data().id, offset)?;
        },
        Err(e) => { Err(e)? },
    };
    metadata::commit_metadata(index)?;
    Ok(())
}

pub fn is_leaf_node(data: &DataProto) -> bool {
    for val in &data.values {
        if val.has_child_id() {
            return false;
        }
    }
    true
}
