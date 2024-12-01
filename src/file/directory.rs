#[cfg(test)]
#[path = "./directory_test.rs"]
mod test;

use std::io::{Read, Write, Seek};
use crate::index::*;
use crate::error::*;
use crate::file::*;
use crate::protos::generated::chunk::*;

fn find_directory_entry<F: Read + Write + Seek>(index: &mut Index<F>, id: u32)
-> Result<(u32, *mut directory_proto::Entry, ChunkProto), Error> {
    for i in 0..index.metadata.num_directories {
        let dir_chunk_offset = i + 1;
        let mut dir_chunk = chunk::read_chunk_at::<F>(
            &index.db_config.file, &mut index.file, dir_chunk_offset)?;
        if !dir_chunk.has_directory() {
            return Err(Error::Internal(format!(
                        "Chunk with offset: {} is not a directory!",
                        dir_chunk_offset).into()));
        }
        for entry in &mut dir_chunk.mut_directory().entries {
            if entry.id == id {
                return Ok((dir_chunk_offset, &mut *entry, dir_chunk));
            }
        }
    }
    Err(Error::NotFound(format!("Chunk with id {} not found.", id)))
}

pub fn find_chunk_offset<F: Read + Write + Seek>(index: &mut Index<F>, id: u32) -> Result<u32, Error> {
    let (_, entry, _) = find_directory_entry::<F>(index, id)?;
    unsafe { Ok((*entry).offset) }
}

pub fn update_chunk_offset<F: Read + Write + Seek>(index: &mut Index<F>, id: u32, offset: u32) -> Result<(), Error> {
    let (chunk_offset, entry, chunk) = find_directory_entry::<F>(index, id)?;
    unsafe { (*entry).offset = offset };
    chunk::write_chunk_at::<F>(&index.db_config.file, &mut index.file, &chunk, chunk_offset)?;
    Ok(())
}

pub fn create_directory_entry<F: Read + Write + Seek>(index: &mut Index<F>, id: u32, offset: u32) -> Result<(), Error> {
    debug_assert_eq!(
        std::mem::discriminant(&find_chunk_offset(index, id).err().unwrap()),
        std::mem::discriminant(&Error::NotFound(String::new())));
    let mut dir_entry = directory_proto::Entry::new();
    dir_entry.id = id;
    dir_entry.offset = offset;

    // find directory with space
    let mut dir_chunk: Option<(ChunkProto, u32)> = None;
    for i in (0..index.metadata.num_directories).rev() {
        let dir_chunk_offset = i + 1;
        let mut test_dir_chunk = chunk::read_chunk_at::<F>(
            &index.db_config.file, &mut index.file, dir_chunk_offset)?;
        if !test_dir_chunk.has_directory() {
            return Err(Error::Internal(format!(
                        "Chunk with offset: {} is not a directory!",
                        dir_chunk_offset).into()));
        }
        if !chunk::would_chunk_overflow(
                &index.db_config.file, &test_dir_chunk, &dir_entry) {
            dir_chunk = Some((test_dir_chunk, dir_chunk_offset));
            break;
        }
    }

    if let Some((mut dir_chunk, dir_chunk_offset)) = dir_chunk {
        log::trace!("Found a directory with existing space: {}", dir_chunk_offset);
        dir_chunk.mut_directory().entries.push(dir_entry);
        chunk::write_chunk_at::<F>(&index.db_config.file, &mut index.file, &dir_chunk, dir_chunk_offset)?;
    }
    else {
        log::trace!("No directory with space available, creating a new one.");

        let mut new_dir_chunk = ChunkProto::new();
        new_dir_chunk.mut_directory().entries.push(dir_entry);

        let new_dir_offset = 1 + index.metadata.num_directories;
        let swap_chunk_offset = index.metadata.next_chunk_offset;
        let swap_chunk = chunk::read_chunk_at::<F>(
            &index.db_config.file, &mut index.file, new_dir_offset)?;
        debug_assert!(swap_chunk.has_data());
        chunk::write_chunk_at::<F>(&index.db_config.file, &mut index.file, &swap_chunk, swap_chunk_offset)?;
        index.metadata.next_chunk_offset += 1;
        directory::update_chunk_offset(index, swap_chunk.data().id, swap_chunk_offset)?;

        chunk::write_chunk_at::<F>(&index.db_config.file, &mut index.file, &new_dir_chunk, new_dir_offset)?;
        index.metadata.num_directories += 1;
    }

    Ok(())
}
