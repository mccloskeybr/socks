use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use std::io::{Read, Seek, Write};

pub fn next_chunk_id<F: Read + Write + Seek>(index: &mut Index<F>) -> u32 {
    let id = index.metadata.next_chunk_id;
    index.metadata.next_chunk_id += 1;
    id
}

pub fn next_chunk_offset<F: Read + Write + Seek>(index: &mut Index<F>) -> u32 {
    let offset = index.metadata.next_chunk_offset;
    index.metadata.next_chunk_offset += 1;
    offset
}

pub fn commit_metadata<F: Read + Write + Seek>(index: &mut Index<F>) -> Result<(), Error> {
    log::trace!("Committing metadata.");
    let mut metadata_chunk = ChunkProto::new();
    metadata_chunk.set_metadata(index.metadata.clone());
    chunk::write_chunk_at::<F>(&index.db_config.file, &mut index.file, &metadata_chunk, 0)?;
    Ok(())
}
