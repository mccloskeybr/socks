use std::io::{Read, Write, Seek};
use crate::protos::generated::chunk::*;
use crate::error::*;
use crate::index::*;
use crate::file::*;

pub fn next_chunk_id<F: Read + Write + Seek>(index: &mut Index<F>) -> u32 {
    let id = index.metadata.next_chunk_id;
    index.metadata.next_chunk_id += 1;
    id
}

pub fn commit_metadata<F: Read + Write + Seek>(index: &mut Index<F>) -> Result<(), Error> {
    log::trace!("Committing metadata.");
    let mut metadata_chunk = ChunkProto::new();
    metadata_chunk.set_metadata(index.metadata.clone());
    chunk::write_chunk_at::<F>(&mut index.file, &metadata_chunk, 0)?;
    index.num_chunk_writes += 1;
    Ok(())
}
