#[cfg(test)]
#[path = "./row_data_test.rs"]
mod test;

use std::io::{Read, Write, Seek};
use crate::index::*;
use crate::error::*;
use crate::file::*;
use crate::protos::generated::chunk::*;

pub fn is_leaf_node(data: &DataProto) -> bool {
    for val in &data.values {
        if val.has_child_id() {
            return false;
        }
    }
    true
}

pub fn find_chunk<F: Read + Write + Seek>(index: &mut Index<F>, id: u32) -> Result<ChunkProto, Error> {
    let offset = directory::find_chunk_offset(index, id)?;
    chunk::read_chunk_at::<F>(&mut index.file, offset)
}

pub fn commit_chunk<F: Read + Write + Seek>(index: &mut Index<F>, chunk: &ChunkProto) -> Result<(), Error> {
    debug_assert!(chunk.has_data());
    log::trace!("Committing chunk: {}.", chunk.data().id);
    match directory::find_chunk_offset(index, chunk.data().id) {
        Ok(chunk_offset) => {
            chunk::write_chunk_at::<F>(&mut index.file, chunk, chunk_offset)?;
        }
        Err(Error::NotFound(..)) => {
            let offset = metadata::next_chunk_offset(index);
            chunk::write_chunk_at::<F>(&mut index.file, chunk, offset)?;
            directory::create_directory_entry(index, chunk.data().id, offset)?;
        },
        Err(e) => { Err(e)? },
    };
    Ok(())
}
