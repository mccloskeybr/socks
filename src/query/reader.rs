use crate::buffer::Buffer;
use crate::error::{ErrorKind::*, *};
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) struct ResultsReader<F: Filelike> {
    file: Arc<Mutex<F>>,
    current_buffer: Buffer<F, InternalQueryResultsProto>,
    current_buffer_offset: u32,
    idx: usize,
}

impl<F: Filelike> ResultsReader<F> {
    pub(crate) fn new(file: F) -> Self {
        let file = Arc::new(Mutex::new(file));
        Self {
            file: file.clone(),
            current_buffer: Buffer::new_for_file(file, 0, InternalQueryResultsProto::new()),
            current_buffer_offset: std::u32::MAX,
            idx: std::usize::MAX,
        }
    }

    // TODO: stages currently read until there is an error, assuming that the first error returned
    // will be of type "the file is done". this assumption likely doesn't always hold.
    pub(crate) async fn next_key(&mut self) -> Result<u32, Error> {
        self.idx = self.idx.wrapping_add(1);
        if self.idx >= self.current_buffer.get().keys.len() {
            self.idx = 0;
            self.current_buffer_offset = self.current_buffer_offset.wrapping_add(1);
            self.current_buffer =
                Buffer::read_from_file(self.file.clone(), self.current_buffer_offset).await?;
            // NOTE: it seems like cursors don't OOB when reading outside written bounds.
            if self.current_buffer.get().keys.len() == 0 {
                return Err(Error::new(OutOfBounds, "".to_string()));
            }
        }
        Ok(self.current_buffer.get().keys[self.idx])
    }
}
