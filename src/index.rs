#[cfg(test)]
#[path = "./index_test.rs"]
mod test;

use std::io::{Read, Write, Seek};
use protobuf::Message;
use protobuf::MessageField;
use crate::error::*;
use crate::transform::*;
use crate::validations::*;
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
    pub num_chunk_writes: u64,
    pub num_chunk_reads: u64,
}

impl<'a, F: 'a + Read + Write + Seek> Index<'a, F> {
    pub fn create(file: &'a mut F, schema: IndexSchema)
    -> Result<Self, Error> {
        validate_schema(&schema)?;

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
            num_chunk_reads: 0,
            num_chunk_writes: 3,
        })
    }

    pub fn open(file: &'a mut F)
    -> Result<Self, Error> {
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
            num_chunk_reads: 0,
            num_chunk_writes: 0,
        })
    }

    // TODO: ensure key doesn't already exist
    // TODO: follow a more efficient splitting algorithm
    pub fn insert(&mut self, op: Insert)
    -> Result<(), Error> {
        let row = transform_insert_op(op, &self.metadata.schema);
        log::trace!("Inserting row: {row}");
        let mut root_chunk = row_data::find_chunk(self, self.metadata.root_chunk_id)?;
        assert!(root_chunk.has_data());
        if chunk::would_chunk_overflow(&root_chunk, &row) {
            log::trace!("Root overflow detected.");

            // TODO: this is inefficient.
            let mut child_chunk = root_chunk.clone();
            child_chunk.mut_data().id = metadata::next_chunk_id(self);

            let mut child_ref = data_proto::Value::new();
            child_ref.set_child_id(child_chunk.data().id);

            root_chunk.mut_data().values.clear();
            root_chunk.mut_data().values.push(child_ref);

            row_data::commit_chunk(self, &child_chunk)?;
            row_data::commit_chunk(self, &root_chunk)?;

            self.split_child(&mut root_chunk, &mut child_chunk, 0)?;
        }
        self.insert_non_full(&mut root_chunk, row)?;
        Ok(())
    }

    fn insert_non_full(&mut self, node_chunk: &mut ChunkProto, row: InternalRowProto)
    -> Result<(), Error> {
        assert!(node_chunk.has_data());
        assert!(!chunk::would_chunk_overflow(node_chunk, &row));
        let node: &mut DataProto = node_chunk.mut_data();

        // find the first value less than row
        let mut insert_pos = 0;
        if !node.values.is_empty() {
            for i in (0..node.values.len()).rev() {
                let value: &data_proto::Value = &node.values[i];
                if value.has_row_node() && value.row_node().key <= row.key {
                    insert_pos = i + 1;
                    break;
                }
            }
        }

        // NOTE: non-leaf nodes will alternate ref / row values.
        // TODO: is this true? can this be out of bounds?
        if row_data::is_leaf_node(node) {
            log::trace!("Inserting row in leaf node: {}.", node.id);
            let mut row_val = data_proto::Value::new();
            row_val.set_row_node(row);
            node_chunk.mut_data().values.insert(insert_pos, row_val);
            row_data::commit_chunk(self, node_chunk)?;
            return Ok(());
        } else {
            let child_idx = insert_pos;
            let mut child_chunk = row_data::find_chunk(self, node.values[child_idx].child_id())?;
            assert!(child_chunk.has_data());

            if chunk::would_chunk_overflow(&child_chunk, &row) {
                // [.., ref, ..] --> [.., ref < val, val, ref > val, .. ]
                self.split_child(node_chunk, &mut child_chunk, child_idx)?;

                // need to determine which split (left, right) to place the value in.
                let node: &DataProto = node_chunk.data();
                let split_val = &node.values[child_idx + 1];
                assert!(split_val.has_row_node());
                if split_val.row_node().key < row.key {
                    child_chunk = row_data::find_chunk(self, node.values[child_idx + 2].child_id())?;
                    assert!(child_chunk.has_data());
                }
            }

            self.insert_non_full(&mut child_chunk, row)?;
            Ok(())
        }
    }

    // splits a child node within a given parent node in half.
    // the split / middle value is brought up to the parent node.
    fn split_child(&mut self, parent_chunk: &mut ChunkProto, left_child_chunk: &mut ChunkProto, child_chunk_idx: usize)
    -> Result<(), Error> {
        assert!(parent_chunk.has_data());
        assert!(left_child_chunk.has_data());
        let parent: &mut DataProto = parent_chunk.mut_data();
        let left_child: &mut DataProto = left_child_chunk.mut_data();

        let mut split_idx = left_child.values.len() / 2;
        if left_child.values[split_idx].has_child_id() {
            split_idx += 1;
            assert!(split_idx < left_child.values.len());
        }
        assert!(left_child.values[split_idx].has_row_node());

        let mut right_child_chunk = ChunkProto::new();
        let mut right_child = right_child_chunk.mut_data();
        right_child.id = metadata::next_chunk_id(self);
        for i in split_idx + 1 .. left_child.values.len() {
            right_child.values.push(left_child.values.remove(split_idx + 1));
        }
        let mut right_child_ref = data_proto::Value::new();
        right_child_ref.set_child_id(right_child.id);

        parent.values.insert(child_chunk_idx + 1, right_child_ref);
        parent.values.insert(child_chunk_idx + 1, left_child.values.remove(split_idx));

        row_data::commit_chunk(self, left_child_chunk)?;
        row_data::commit_chunk(self, &right_child_chunk)?;
        row_data::commit_chunk(self, parent_chunk)?;
        metadata::commit_metadata(self)?;

        Ok(())
    }
}
