#[cfg(test)]
#[path = "./table_test.rs"]
mod test;

use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::table::bp_tree;
use crate::table::cache::Cache;
use crate::table::file::*;
use crate::table::parse::*;
use protobuf::Message;
use protobuf::MessageField;
use std::io::{Read, Seek, Write};

// Table file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub struct Table<'a, F: 'a + Read + Write + Seek> {
    pub file: &'a mut F,
    pub cache: Cache,
    pub metadata: TableMetadataProto,
    pub db_config: DatabaseConfig,
}

impl<'a, F: 'a + Read + Write + Seek> Table<'a, F> {
    pub fn create(
        file: &'a mut F,
        db_config: DatabaseConfig,
        table_config: TableConfig,
    ) -> Result<Self, Error> {
        validate::schema(&table_config.schema)?;

        let mut metadata = TableMetadataProto::new();
        metadata.config = MessageField::some(table_config.clone());
        metadata.root_chunk_offset = 1;
        metadata.next_chunk_offset = 2;
        {
            let mut metadata_chunk = ChunkProto::new();
            metadata_chunk.set_metadata(metadata.clone());
            chunk::write_chunk_at::<F>(&db_config.file, file, metadata_chunk, 0)?;
        }
        {
            let mut root_node_chunk = ChunkProto::new();
            let root_node: &mut NodeProto = root_node_chunk.mut_node();
            root_node.offset = 1;
            root_node.set_internal(InternalNodeProto::new());
            chunk::write_chunk_at::<F>(&db_config.file, file, root_node_chunk, 1)?;
        }

        Ok(Self {
            file: file,
            cache: Cache::default(),
            metadata: metadata,
            db_config: db_config,
        })
    }

    pub fn insert(&mut self, op: InsertProto) -> Result<(), Error> {
        // TODO: validate op
        let (key, row) = transform::insert_op(op, &self.metadata.config.schema);
        log::trace!("Inserting row: {row}");
        bp_tree::insert(self, key, row)
    }

    pub fn read_row(&mut self, op: ReadRowProto) -> Result<InternalRowProto, Error> {
        // TODO: validate op
        let key: u32 = transform::read_row_op(op, &self.metadata.config.schema);
        bp_tree::read_row(self, self.metadata.root_chunk_offset, key)
    }
}
