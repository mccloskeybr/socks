use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use std::io::{Read, Seek, Write};

pub fn read_chunk<F: Read + Write + Seek>(
    index: &mut Index<F>,
    offset: u32,
) -> Result<ChunkProto, Error> {
    chunk::read_chunk_at::<F>(&index.db_config.file, &mut index.file, offset)
}

pub fn commit_chunk<F: Read + Write + Seek>(
    index: &mut Index<F>,
    chunk: &ChunkProto,
) -> Result<(), Error> {
    debug_assert!(chunk.has_node());
    log::trace!("Committing chunk: {}.", chunk.node().offset);
    chunk::write_chunk_at::<F>(
        &index.db_config.file,
        &mut index.file,
        chunk,
        chunk.node().offset,
    )
}
