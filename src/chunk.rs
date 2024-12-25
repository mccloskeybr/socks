#[cfg(test)]
#[path = "./chunk_test.rs"]
mod test;

use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::stats;
use crate::{CHUNK_OVERFLOW_BUFFER, CHUNK_SIZE};
use protobuf::Message;
use std::io::{Read, Seek, SeekFrom, Write};

// Byte format of each chunk is the following:
// 1. data size: u16 / 4 bytes.
// 2. data: [u8] proto message to end of section.
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

fn chunk_from_bytes<M: Message>(bytes: &[u8]) -> Result<M, Error> {
    let mut cursor: usize = 0;

    let slice = read_data_update_cursor(bytes, std::mem::size_of::<u16>(), &mut cursor)?;
    let chunk_size = u16::from_be_bytes(slice.try_into()?);

    let slice = read_data_update_cursor(bytes, chunk_size as usize, &mut cursor)?;
    let chunk = M::parse_from_bytes(&slice)?;

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

fn chunk_to_bytes<M: Message>(msg: &M) -> Result<Vec<u8>, Error> {
    let data: Vec<u8> = msg.write_to_bytes()?;
    let data_len: u16 = data.len().try_into().unwrap();

    let mut bytes = Vec::with_capacity(CHUNK_SIZE as usize);
    bytes.resize_with(CHUNK_SIZE as usize, || 0);
    let mut cursor: usize = 0;
    write_data_update_cursor(&data_len.to_be_bytes(), &mut bytes, &mut cursor)?;
    write_data_update_cursor(&data, &mut bytes, &mut cursor)?;

    Ok(bytes)
}

pub(crate) fn read_chunk_at<F: Filelike, M: Message>(
    reader: &mut F,
    chunk_offset: u32,
) -> Result<M, Error> {
    let mut bytes: Vec<u8> = Vec::with_capacity(CHUNK_SIZE as usize);
    bytes.resize_with(CHUNK_SIZE as usize, || 0);
    reader.seek(SeekFrom::Start(chunk_offset as u64 * CHUNK_SIZE as u64))?;
    reader.read(&mut bytes)?;
    stats::increment_chunk_read();
    chunk_from_bytes::<M>(&bytes)
}

pub(crate) fn write_chunk_at<F: Filelike, M: Message>(
    writer: &mut F,
    msg: M,
    chunk_offset: u32,
) -> Result<(), Error> {
    let bytes: Vec<u8> = chunk_to_bytes::<M>(&msg)?;
    writer.seek(SeekFrom::Start(chunk_offset as u64 * CHUNK_SIZE as u64))?;
    writer.write(&bytes)?;
    writer.flush()?;
    stats::increment_chunk_write();
    Ok(())
}

pub(crate) fn would_chunk_overflow(size: usize) -> bool {
    let size_estimate = std::mem::size_of::<u16>() + size + CHUNK_OVERFLOW_BUFFER as usize;
    CHUNK_SIZE <= size_estimate.try_into().unwrap()
}
