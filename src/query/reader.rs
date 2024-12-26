use crate::chunk;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;

pub(crate) struct ResultsReader<F: Filelike> {
    file: F,
    current_chunk: InternalQueryResultsProto,
    current_chunk_offset: u32,
    idx: usize,
}

impl<F: Filelike> ResultsReader<F> {
    pub(crate) fn new(file: F) -> Self {
        Self {
            file: file,
            current_chunk: InternalQueryResultsProto::new(),
            current_chunk_offset: std::u32::MAX,
            idx: std::usize::MAX,
        }
    }

    // TODO: stages currently read until there is an error, assuming that the first error returned
    // will be of type "the file is done". this assumption likely doesn't always hold.
    pub(crate) async fn next_key(&mut self) -> Result<u32, Error> {
        self.idx = self.idx.wrapping_add(1);
        if self.idx >= self.current_chunk.keys.len() {
            self.idx = 0;
            self.current_chunk_offset = self.current_chunk_offset.wrapping_add(1);
            dbg!("{}", self.current_chunk_offset);
            self.current_chunk =
                chunk::read_chunk_at(&mut self.file, self.current_chunk_offset).await?;
            // NOTE: it seems like cursors don't OOB when reading outside written bounds?
            if self.current_chunk.keys.len() == 0 {
                return Err(Error::OutOfBounds("".to_string()));
            }
        }
        Ok(self.current_chunk.keys[self.idx])
    }
}
