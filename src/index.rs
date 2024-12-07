#[cfg(test)]
#[path = "./index_test.rs"]
mod test;

use std::io::{Read, Write, Seek};
use protobuf::Message;
use protobuf::MessageField;
use crate::bp_tree;
use crate::error::*;
use crate::parse::*;
use crate::file::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::config::*;
use crate::protos::generated::chunk::*;

// Index file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub struct Index<'a, F: 'a + Read + Write + Seek> {
    pub file: &'a mut F,
    pub metadata: IndexMetadataProto,
    pub db_config: DatabaseConfig,
}

impl<'a, F: 'a + Read + Write + Seek> Index<'a, F> {
    pub fn create(
        file: &'a mut F, db_config: DatabaseConfig, index_config: IndexConfig
    ) -> Result<Self, Error> {
        validate::schema(&index_config.schema)?;

        let mut metadata = IndexMetadataProto::new();
        metadata.config = MessageField::some(index_config.clone());
        metadata.next_chunk_id = 1;
        metadata.next_chunk_offset = 3;
        metadata.root_chunk_id = 0;
        metadata.num_directories = 1;
        {
            let mut metadata_chunk = ChunkProto::new();
            metadata_chunk.set_metadata(metadata.clone());
            chunk::write_chunk_at::<F>(&db_config.file, file, &metadata_chunk, 0)?;
        }
        {
            let mut root_node_entry = directory_proto::Entry::new();
            root_node_entry.id = 0;
            root_node_entry.offset = 2;
            let mut directory_chunk = ChunkProto::new();
            let directory: &mut DirectoryProto = directory_chunk.mut_directory();
            directory.entries.push(root_node_entry);
            chunk::write_chunk_at::<F>(&db_config.file, file, &directory_chunk, 1)?;
        }
        {
            let mut root_node_chunk = ChunkProto::new();
            let root_node: &mut NodeProto = root_node_chunk.mut_node();
            root_node.id = 0;
            root_node.set_internal(InternalNodeProto::new());
            chunk::write_chunk_at::<F>(&db_config.file, file, &root_node_chunk, 2)?;
        }

        Ok(Self {
            file: file,
            metadata: metadata,
            db_config: db_config,
        })
    }

    pub fn insert(&mut self, op: InsertProto) -> Result<(), Error> {
        // TODO: validate op
        let row = transform::insert_op(op, &self.metadata.config.schema);
        log::trace!("Inserting row: {row}");

        match self.metadata.config.insert_method.enum_value_or_default() {
            index_config::InsertMethod::AGGRESSIVE_SPLIT => bp_tree::insert_aggressive_split::insert(self, row),
        }
    }

    pub fn read_row(&mut self, op: ReadRowProto) -> Result<InternalRowProto, Error> {
        // TODO: validate op
        let key: String = transform::read_row_op(op, &self.metadata.config.schema);
        bp_tree::read::read_row(self, self.metadata.root_chunk_id, key)
    }
}
