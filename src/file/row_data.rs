use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use std::io::{Read, Seek, Write};

pub fn find_chunk<F: Read + Write + Seek>(
    index: &mut Index<F>,
    id: u32,
) -> Result<ChunkProto, Error> {
    let offset = directory::find_chunk_offset(index, id)?;
    chunk::read_chunk_at::<F>(&index.db_config.file, &mut index.file, offset)
}

pub fn commit_chunk<F: Read + Write + Seek>(
    index: &mut Index<F>,
    chunk: &ChunkProto,
) -> Result<(), Error> {
    debug_assert!(chunk.has_node());
    log::trace!("Committing chunk: {}.", chunk.node().id);
    match directory::find_chunk_offset(index, chunk.node().id) {
        Ok(chunk_offset) => {
            chunk::write_chunk_at::<F>(
                &index.db_config.file,
                &mut index.file,
                chunk,
                chunk_offset,
            )?;
        }
        Err(Error::NotFound(..)) => {
            let offset = metadata::next_chunk_offset(index);
            chunk::write_chunk_at::<F>(&index.db_config.file, &mut index.file, chunk, offset)?;
            directory::create_directory_entry(index, chunk.node().id, offset)?;
        }
        Err(e) => Err(e)?,
    };
    Ok(())
}
