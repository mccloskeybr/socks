#[cfg(test)]
#[path = "./table_test.rs"]
mod test;

use crate::bp_tree;
use crate::cache::Cache;
use crate::chunk;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use protobuf::Message;
use protobuf::MessageField;
use std::io::{Read, Seek, Write};

// Table file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub(crate) struct Table<F: Filelike> {
    pub(crate) file: F,
    pub(crate) metadata: TableMetadataProto,
}

impl<F: Filelike> Table<F> {
    pub(crate) fn next_chunk_offset(&mut self) -> u32 {
        let offset = self.metadata.next_chunk_offset;
        self.metadata.next_chunk_offset += 1;
        offset
    }

    pub(crate) fn is_table_keyed_on_column(&self, col_name: &str) -> bool {
        &self.metadata.schema.key.name == col_name
    }

    pub(crate) fn commit_metadata(&mut self) -> Result<(), Error> {
        log::trace!("Committing metadata.");
        chunk::write_chunk_at(&mut self.file, self.metadata.clone(), 0)?;
        Ok(())
    }

    pub(crate) fn create(
        mut file: F,
        name: String,
        id: u32,
        schema: TableSchema,
    ) -> Result<Self, Error> {
        let mut metadata = TableMetadataProto::new();
        metadata.name = name;
        metadata.id = id;
        metadata.schema = MessageField::some(schema);
        metadata.root_chunk_offset = 1;
        metadata.next_chunk_offset = 2;
        {
            chunk::write_chunk_at(&mut file, metadata.clone(), 0)?;
        }
        {
            let mut root_node = NodeProto::new();
            root_node.offset = 1;
            root_node.set_internal(InternalNodeProto::new());
            chunk::write_chunk_at(&mut file, root_node, 1)?;
        }
        Ok(Self {
            file: file,
            metadata: metadata,
        })
    }

    pub(crate) fn insert(
        &mut self,
        cache: &mut Cache,
        key: u32,
        row: InternalRowProto,
    ) -> Result<(), Error> {
        log::trace!("Inserting row: {row}");
        bp_tree::insert(self, cache, key, row)
    }

    pub(crate) fn read_row(&mut self, cache: &mut Cache, key: u32) -> Result<RowProto, Error> {
        log::trace!("Retrieving row with key: {key}");
        let internal_row = bp_tree::read_row(self, cache, self.metadata.root_chunk_offset, key)?;
        Ok(schema::internal_row_to_row(
            &internal_row,
            &self.metadata.schema,
        ))
    }
}
