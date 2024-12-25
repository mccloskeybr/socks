use crate::chunk;
use crate::database::*;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::operations::*;
use crate::table::Table;
use protobuf::Message;
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) struct ResultsWriter<F: Filelike> {
    pub(crate) file: F,
    current_chunk: InternalQueryResultsProto,
    current_chunk_offset: u32,
}

impl<F: Filelike> ResultsWriter<F> {
    pub(crate) fn new(mut file: F) -> Self {
        Self {
            file: file,
            current_chunk: InternalQueryResultsProto::new(),
            current_chunk_offset: 0,
        }
    }

    pub(crate) fn write_key(&mut self, key: u32) -> Result<(), Error> {
        if chunk::would_chunk_overflow(
            self.current_chunk.compute_size() as usize + std::mem::size_of::<u32>(),
        ) {
            chunk::write_chunk_at(
                &mut self.file,
                self.current_chunk.clone(),
                self.current_chunk_offset,
            )?;
            self.current_chunk_offset += 1;
            self.current_chunk = InternalQueryResultsProto::new();
        }
        self.current_chunk.keys.push(key);
        Ok(())
    }

    pub(crate) fn write_key_row(&mut self, key: u32, row: RowProto) -> Result<(), Error> {
        if chunk::would_chunk_overflow(
            self.current_chunk.compute_size() as usize
                + row.compute_size() as usize
                + std::mem::size_of::<u32>(),
        ) {
            chunk::write_chunk_at(
                &mut self.file,
                self.current_chunk.clone(),
                self.current_chunk_offset,
            )?;
            self.current_chunk_offset += 1;
            self.current_chunk = InternalQueryResultsProto::new();
        }
        self.current_chunk.keys.push(key);
        self.current_chunk.rows.push(row);
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<(), Error> {
        chunk::write_chunk_at(
            &mut self.file,
            self.current_chunk.clone(),
            self.current_chunk_offset,
        )?;
        Ok(())
    }
}
