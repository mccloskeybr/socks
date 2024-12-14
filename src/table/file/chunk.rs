#[cfg(test)]
#[path = "./chunk_test.rs"]
mod test;

use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::stats;
use protobuf::Message;
use std::io::{Read, Seek, SeekFrom, Write};

// Byte format of each chunk is the following:
// 1. data size: u16 / 4 bytes.
// 2. data: [u8] ChunkProto to end of section.
// Each section is guaranteed to be of a configurable static size.

fn read_data_update_cursor<'a>(
    src: &'a [u8],
    size: usize,
    cursor: &mut usize,
) -> Result<&'a [u8], Error> {
    let Some(slice) = src.get(*cursor..*cursor + size) else {
        return Err(Error::OutOfBounds(
            format!("Requested size is too large for chunk!: {}", size).into(),
        ));
    };
    *cursor += size;
    Ok(slice)
}

fn chunk_from_bytes(bytes: &[u8]) -> Result<ChunkProto, Error> {
    let mut cursor: usize = 0;

    let slice = read_data_update_cursor(bytes, std::mem::size_of::<u16>(), &mut cursor)?;
    let chunk_size = u16::from_be_bytes(slice.try_into()?);

    let slice = read_data_update_cursor(bytes, chunk_size as usize, &mut cursor)?;
    let chunk = ChunkProto::parse_from_bytes(&slice)?;

    Ok(chunk)
}

fn write_data_update_cursor(
    src: &[u8],
    dest: &mut Vec<u8>,
    cursor: &mut usize,
) -> Result<(), Error> {
    let Some(slice) = dest.get_mut(*cursor..*cursor + src.len()) else {
        return Err(Error::OutOfBounds(
            format!(
                "Source data of size: {} cannot fit in dest chunk of size {} at pos {}!",
                src.len(),
                dest.len(),
                cursor
            )
            .into(),
        ));
    };
    slice.copy_from_slice(src);
    *cursor += src.len();
    Ok(())
}

fn chunk_to_bytes(config: &FileConfig, chunk: &ChunkProto) -> Result<Vec<u8>, Error> {
    let data: Vec<u8> = chunk.write_to_bytes()?;
    let data_len: u16 = data.len().try_into().unwrap();

    let mut bytes = Vec::with_capacity(config.chunk_size as usize);
    bytes.resize_with(config.chunk_size as usize, || 0);
    let mut cursor: usize = 0;
    write_data_update_cursor(&data_len.to_be_bytes(), &mut bytes, &mut cursor)?;
    write_data_update_cursor(&data, &mut bytes, &mut cursor)?;

    Ok(bytes)
}

pub fn read_chunk_at<R: Read + Seek>(
    config: &FileConfig,
    reader: &mut R,
    chunk_offset: u32,
) -> Result<ChunkProto, Error> {
    let mut bytes: Vec<u8> = Vec::with_capacity(config.chunk_size as usize);
    bytes.resize_with(config.chunk_size as usize, || 0);
    reader.seek(SeekFrom::Start(
        chunk_offset as u64 * config.chunk_size as u64,
    ))?;
    reader.read(&mut bytes)?;
    stats::increment_chunk_read();
    chunk_from_bytes(&bytes)
}

pub fn write_chunk_at<W: Write + Seek>(
    config: &FileConfig,
    writer: &mut W,
    chunk: ChunkProto,
    chunk_offset: u32,
) -> Result<(), Error> {
    let bytes: Vec<u8> = chunk_to_bytes(config, &chunk)?;
    writer.seek(SeekFrom::Start(
        chunk_offset as u64 * config.chunk_size as u64,
    ))?;
    writer.write(&bytes)?;
    writer.flush()?;
    stats::increment_chunk_write();
    Ok(())
}

pub fn would_chunk_overflow(config: &FileConfig, size: usize) -> bool {
    let size_estimate = std::mem::size_of::<u16>() + size + config.chunk_overflow_size as usize;
    config.chunk_size <= size_estimate.try_into().unwrap()
}
