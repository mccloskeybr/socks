use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::table::file::*;
use crate::table::table::*;
use std::io::{Read, Seek, Write};

pub fn read_chunk<F: Read + Write + Seek>(
    table: &mut Table<F>,
    offset: u32,
) -> Result<ChunkProto, Error> {
    chunk::read_chunk_at::<F>(&table.db_config.file, &mut table.file, offset)
}

pub fn commit_chunk<F: Read + Write + Seek>(
    table: &mut Table<F>,
    chunk: &ChunkProto,
) -> Result<(), Error> {
    debug_assert!(chunk.has_node());
    log::trace!("Committing chunk: {}.", chunk.node().offset);
    chunk::write_chunk_at::<F>(
        &table.db_config.file,
        &mut table.file,
        chunk,
        chunk.node().offset,
    )
}
