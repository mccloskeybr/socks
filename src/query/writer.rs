use crate::buffer::Buffer;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::operations::*;
use protobuf::Message;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) struct ResultsWriter<F: Filelike> {
    current_buffer: Buffer<F, InternalQueryResultsProto>,
    current_buffer_offset: u32,
}

impl<F: Filelike> ResultsWriter<F> {
    pub(crate) fn new(file: F) -> Self {
        Self {
            current_buffer: Buffer::new_for_file(
                Arc::new(Mutex::new(file)),
                0,
                InternalQueryResultsProto::new(),
            ),
            current_buffer_offset: 0,
        }
    }

    pub(crate) async fn write_key(&mut self, key: u32) -> Result<(), Error> {
        if self
            .current_buffer
            .would_overflow(std::mem::size_of::<u32>())
        {
            let file = self.current_buffer.file.clone();
            self.current_buffer.write_to_file().await?;
            self.current_buffer_offset += 1;
            self.current_buffer = Buffer::new_for_file(
                file,
                self.current_buffer_offset,
                InternalQueryResultsProto::new(),
            );
        }
        self.current_buffer.get_mut().keys.push(key);
        Ok(())
    }

    pub(crate) async fn write_key_row(&mut self, key: u32, row: RowProto) -> Result<(), Error> {
        if self
            .current_buffer
            .would_overflow(row.compute_size() as usize + std::mem::size_of::<u32>())
        {
            let file = self.current_buffer.file.clone();
            self.current_buffer.write_to_file().await?;
            self.current_buffer_offset += 1;
            self.current_buffer = Buffer::new_for_file(
                file,
                self.current_buffer_offset,
                InternalQueryResultsProto::new(),
            );
        }
        self.current_buffer.get_mut().keys.push(key);
        self.current_buffer.get_mut().rows.push(row);
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<F, Error> {
        self.current_buffer.write_to_file().await?;
        Ok(Arc::into_inner(self.current_buffer.file)
            .unwrap()
            .into_inner())
    }
}
