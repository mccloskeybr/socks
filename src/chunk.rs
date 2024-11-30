#[cfg(test)]
#[path = "./chunk_test.rs"]
mod test;

use std::io::{Read, Write, Seek, SeekFrom};
use protobuf::Message;
use crate::error::*;
use crate::protos::generated::chunk::*;

// Byte format of each chunk is the following:
// 1. data size: u64 / 8 bytes.
// 2. data: [u8] ChunkProto to end of section.
// Each section is guaranteed to be of size CHUNK_SIZE.

// NOTE: the maximum size of each chunk on disk.
pub const CHUNK_SIZE: usize = 512;

// NOTE: the amount of space remaining to consider a chunk as full.
// required because protos are sized dynamically and true length
// cannot be determined without properly encoding it.
// allowing some small buffer is (probably) more efficient than
// computing the actual size every time? TODO test.
pub const CHUNK_OVERFLOW_BUFFER: usize = 25;

fn read_data_update_cursor<'a>(
    src: &'a [u8; CHUNK_SIZE], size: usize, cursor: &mut usize)
-> Result<&'a [u8], Error> {
    let Some(slice) = src.get(*cursor .. *cursor + size) else {
        return Err(Error::OutOfBounds(format!(
                    "Requested size is too large for chunk!: {}",
                    size).into()));
    };
    *cursor += size;
    Ok(slice)
}

fn chunk_from_bytes(bytes: &[u8; CHUNK_SIZE])
-> Result<Chunk, Error> {
    let mut cursor: usize = 0;

    let slice = read_data_update_cursor(bytes, std::mem::size_of::<u64>(), &mut cursor)?;
    let chunk_size: u64 = u64::from_be_bytes(slice.try_into()?);

    let slice = read_data_update_cursor(bytes, chunk_size as usize, &mut cursor)?;
    let chunk = Chunk::parse_from_bytes(&slice)?;

    Ok(chunk)
}

fn write_data_update_cursor(
    src: &[u8], dest: &mut [u8; CHUNK_SIZE], cursor: &mut usize)
-> Result<(), Error> {
    let Some(slice) = dest.get_mut(*cursor .. *cursor + src.len()) else {
        return Err(Error::OutOfBounds(format!(
                    "Source data of size: {} cannot fit in dest chunk of size {} at pos {}!",
                    src.len(), dest.len(), cursor).into()));
    };
    slice.copy_from_slice(src);
    *cursor += src.len();
    Ok(())
}

fn chunk_to_bytes(chunk: &Chunk)
-> Result<[u8; CHUNK_SIZE], Error> {
    let data: Vec<u8> = chunk.write_to_bytes()?;
    let data_len: usize = data.len();

    let mut chunk = [0; CHUNK_SIZE];
    let mut cursor: usize = 0;
    write_data_update_cursor(&data_len.to_be_bytes(), &mut chunk, &mut cursor)?;
    write_data_update_cursor(&data, &mut chunk, &mut cursor)?;

    Ok(chunk)
}

pub fn read_chunk_at<R: Read + Seek>(reader: &mut R, chunk_offset: u64)
-> Result<Chunk, Error> {
    let mut chunk_bytes: [u8; CHUNK_SIZE] = [0; CHUNK_SIZE];
    reader.seek(SeekFrom::Start(chunk_offset * CHUNK_SIZE as u64))?;
    reader.read(&mut chunk_bytes)?;
    chunk_from_bytes(&chunk_bytes)
}


pub fn write_chunk_at<W: Write + Seek>(writer: &mut W, chunk: &Chunk, chunk_offset: u64)
-> Result<(), Error> {
    let chunk_bytes: [u8; CHUNK_SIZE] = chunk_to_bytes(chunk)?;
    writer.seek(SeekFrom::Start(chunk_offset * CHUNK_SIZE as u64))?;
    writer.write(&chunk_bytes)?;
    writer.flush()?;
    Ok(())
}

pub fn would_chunk_overflow<M: Message>(chunk: &Chunk, msg: &M)
-> bool {
    let size_estimate =
        std::mem::size_of::<u64>() +
        chunk.compute_size() as usize +
        msg.compute_size() as usize +
        CHUNK_OVERFLOW_BUFFER;
    CHUNK_SIZE <= size_estimate
}

pub fn is_leaf_node(row_data: &RowDataNode) -> bool {
    for val in &row_data.values {
        if val.has_child_id() {
            return false;
        }
    }
    true
}
