use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::table::file::*;
use crate::table::table::*;
use std::io::{Read, Seek, Write};

pub fn next_chunk_offset<F: Read + Write + Seek>(table: &mut Table<F>) -> u32 {
    let offset = table.metadata.next_chunk_offset;
    table.metadata.next_chunk_offset += 1;
    offset
}

pub fn commit_metadata<F: Read + Write + Seek>(table: &mut Table<F>) -> Result<(), Error> {
    log::trace!("Committing metadata.");
    let mut metadata_chunk = ChunkProto::new();
    metadata_chunk.set_metadata(table.metadata.clone());
    chunk::write_chunk_at::<F>(&table.db_config.file, &mut table.file, &metadata_chunk, 0)?;
    Ok(())
}
