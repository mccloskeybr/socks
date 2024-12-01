#[cfg(test)]
#[path = "./index_test.rs"]
mod test;

use std::io::{Read, Write, Seek};
use protobuf::Message;
use protobuf::MessageField;
use crate::b_tree;
use crate::error::*;
use crate::parse::*;
use crate::file::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::schema::*;
use crate::protos::generated::chunk::*;

// Index file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub struct Index<'a, F: 'a + Read + Write + Seek> {
    pub file: &'a mut F,
    pub metadata: MetadataProto,
}

impl<'a, F: 'a + Read + Write + Seek> Index<'a, F> {
    pub fn create(file: &'a mut F, schema: IndexSchema) -> Result<Self, Error> {
        validate::schema(&schema)?;

        let mut metadata = MetadataProto::new();
        metadata.schema = MessageField::some(schema.clone());
        metadata.next_chunk_id = 1;
        metadata.next_chunk_offset = 3;
        metadata.root_chunk_id = 0;
        metadata.num_directories = 1;
        {
            let mut metadata_chunk = ChunkProto::new();
            metadata_chunk.set_metadata(metadata.clone());
            chunk::write_chunk_at::<F>(file, &metadata_chunk, 0)?;
        }
        {
            let mut root_node_entry = directory_proto::Entry::new();
            root_node_entry.id = 0;
            root_node_entry.offset = 2;
            let mut directory_chunk = ChunkProto::new();
            let directory: &mut DirectoryProto = directory_chunk.mut_directory();
            directory.entries.push(root_node_entry);
            chunk::write_chunk_at::<F>(file, &directory_chunk, 1)?;
        }
        {
            let mut root_node_chunk = ChunkProto::new();
            let root_node: &mut DataProto = root_node_chunk.mut_data();
            root_node.id = 0;
            chunk::write_chunk_at::<F>(file, &root_node_chunk, 2)?;
        }

        Ok(Self {
            file: file,
            metadata: metadata,
        })
    }

    pub fn open(file: &'a mut F) -> Result<Self, Error> {
        let mut chunk: ChunkProto = chunk::read_chunk_at::<F>(file, 0)?;
        if !chunk.has_metadata() {
            return Err(Error::InvalidArgument(
                    "First chunk not metadata as expected!".into()));
        }
        let metadata: MetadataProto = chunk.take_metadata();
        if metadata.schema.is_none() {
            return Err(Error::InvalidArgument(
                    "No schema present!".into()));
        }
        Ok(Self {
            file: file,
            metadata: metadata,
        })
    }

    pub fn insert(&mut self, op: Insert) -> Result<(), Error> {
        // TODO: validate op
        let row: InternalRowProto = transform::insert_op(op, &self.metadata.schema);
        log::trace!("Inserting row: {row}");

        // TODO: this algorithm splits greedily -- follow a better one
        b_tree::cormen_insert::insert(self, row)
    }
}
