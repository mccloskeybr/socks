#[cfg(test)]
#[path = "./buffer_test.rs"]
mod test;

use crate::error::*;
use crate::filelike::Filelike;
use crate::table::Table;
use crate::{BUFFER_OVERFLOW_BUFFER, BUFFER_SIZE};
use protobuf::Message;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

// Buffers represent a single page, chunk, etc. of data stored on disk. Each chunk
// is guaranteed to be of a static size BUFFER_SIZE. Each buffer stores a single
// protobuf message, e.g. a B+ tree node, or table metadata, etc. Not thread safe /
// intended to be accessed behind some locking mechanism.
//
// Byte format of each buffer is the following:
// 1. data size: u16 / 2 bytes.
// 2. data: [u8] proto message to end of section.
#[derive(Debug)]
pub(crate) struct Buffer<F: Filelike, M: Message> {
    pub(crate) file: Arc<Mutex<F>>,
    pub(crate) offset: u32,
    pub(crate) data: M,
    pub(crate) is_dirty: bool,
}

impl<F: Filelike, M: Message> Buffer<F, M> {
    // Writes all bytes from src into dest at cursor. Increments cursor by the size of src.
    fn write_bytes(
        src: &[u8],
        dest: &mut [u8; BUFFER_SIZE],
        cursor: &mut usize,
    ) -> Result<(), Error> {
        let Some(slice) = dest.get_mut(*cursor..*cursor + src.len()) else {
            return Err(Error::OutOfBounds(
                format!(
                    "Source data of size: {} cannot fit in dest buffer of size {} at pos {}!",
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

    // Transforms the given protobuf message into a buffer byte array.
    fn message_to_bytes(msg: &M) -> Result<[u8; BUFFER_SIZE], Error> {
        let data: Vec<u8> = msg.write_to_bytes()?;
        let data_len: u16 = data.len().try_into().unwrap();
        let mut bytes = [0; BUFFER_SIZE];
        let mut cursor: usize = 0;
        Self::write_bytes(&data_len.to_be_bytes(), &mut bytes, &mut cursor)?;
        Self::write_bytes(&data, &mut bytes, &mut cursor)?;
        Ok(bytes)
    }

    // Reads and returns size bytes from the provided src buffer. Increments Cursor by size.
    // Intended to consume a byte array / transform into a known structure.
    fn read_slice<'a>(src: &'a [u8], size: usize, cursor: &mut usize) -> Result<&'a [u8], Error> {
        let Some(slice) = src.get(*cursor..*cursor + size) else {
            return Err(Error::OutOfBounds(
                format!("Requested size is too large for buffer!: {}", size).into(),
            ));
        };
        *cursor += size;
        Ok(slice)
    }

    // Interprets the given buffer bytes as a buffer protobuf message.
    fn message_from_bytes(bytes: &[u8]) -> Result<M, Error> {
        let mut cursor: usize = 0;
        let slice = Self::read_slice(bytes, std::mem::size_of::<u16>(), &mut cursor)?;
        let buffer_size = u16::from_be_bytes(slice.try_into()?);
        let slice = Self::read_slice(bytes, buffer_size as usize, &mut cursor)?;
        Ok(M::parse_from_bytes(&slice)?)
    }

    // Creates an empty buffer associated with the given file / offset.
    pub(crate) fn new_for_file(file: Arc<Mutex<F>>, offset: u32, data: M) -> Self {
        Self {
            file: file,
            offset: offset,
            data: data,
            is_dirty: true,
        }
    }

    // Claims the next offset for the given table and creates an empty buffer
    // at that location.
    pub(crate) async fn new_next_for_table(table: &Table<F>) -> Self {
        Self::new_for_file(table.file.clone(), table.next_chunk_offset(), M::new())
    }

    // Reads the buffer at the given file / offset and returns it.
    pub(crate) async fn read_from_file(file: Arc<Mutex<F>>, offset: u32) -> Result<Self, Error> {
        let mut bytes = [0; BUFFER_SIZE];
        {
            let mut file_lock = file.lock().await;
            file_lock
                .seek(SeekFrom::Start(offset as u64 * BUFFER_SIZE as u64))
                .await?;
            file_lock.read(&mut bytes).await?;
        }
        Ok(Self {
            file: file,
            offset: offset,
            data: Self::message_from_bytes(&bytes)?,
            is_dirty: false,
        })
    }

    // Reads the buffer at the given table's file / offset and returns it.
    pub(crate) async fn read_from_table(table: &Table<F>, offset: u32) -> Result<Self, Error> {
        Self::read_from_file(table.file.clone(), offset).await
    }

    // Writes the buffer's current contents to its configured location.
    // NOTE: Expects that the buffer does not exceed the static size limit.
    pub(crate) async fn write_to_file(&mut self) -> Result<(), Error> {
        assert!(!self.would_overflow(0));
        if !self.is_dirty {
            return Ok(());
        }
        let bytes: [u8; BUFFER_SIZE] = Self::message_to_bytes(&self.data)?;
        {
            let mut file_lock = self.file.lock().await;
            file_lock
                .seek(SeekFrom::Start(self.offset as u64 * BUFFER_SIZE as u64))
                .await?;
            file_lock.write(&bytes).await?;
            file_lock.flush().await?;
        }
        Ok(())
    }

    // Returns true iff adding the provided size to the buffer will exceed
    // the static size limit.
    pub(crate) fn would_overflow(&self, addl_size: usize) -> bool {
        let size_estimate = std::mem::size_of::<u16>()
            + self.data.compute_size() as usize
            + addl_size
            + BUFFER_OVERFLOW_BUFFER as usize;
        BUFFER_SIZE <= size_estimate.try_into().unwrap()
    }

    // Retrieve an immutable reference to the underlying proto.
    pub(crate) fn get<'a>(&'a self) -> &'a M {
        &self.data
    }

    // Retrieve a mutable reference to the underlying proto.
    // Silently marks the buffer as having uncommitted changes.
    pub(crate) fn get_mut<'a>(&'a mut self) -> &'a mut M {
        self.is_dirty = true;
        &mut self.data
    }
}
