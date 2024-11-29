use crate::chunk::*;
use crate::validations::*;
use std::fs::File;
use protobuf::Message;
use protobuf::MessageField;
use crate::error::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::schema::*;
use crate::protos::generated::chunk::*;

// Index file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub struct Index {
    file: File,
    metadata: Metadata,
}

impl Index {
    fn next_chunk_id(&mut self) -> u64 {
        let id = self.metadata.next_chunk_id;
        self.metadata.next_chunk_id += 1;
        id
    }

    // NOTE: searches directory chunks to find the mapping of chunk_id --> chunk_offset.
    fn find_offset_for_chunk(&mut self, chunk_id: u64)
    -> Result<u64, Error> {
        let mut chunk_idx = 0;
        loop {
            let mut chunk = read_chunk_at(&mut self.file, chunk_idx)?;
            if !chunk.has_directory() {
                return Err(Error::InvalidArgument("Chunk is not a directory!".into()));
            }
            let directory = chunk.take_directory();
            for entry in directory.entries {
                if entry.id == chunk_id {
                    return Ok(entry.offset);
                }
            }
            chunk_idx += 1;
        }
        unreachable!();
    }

    fn find_chunk(&mut self, chunk_id: u64)
    -> Result<Chunk, Error> {
        let chunk_offset = self.find_offset_for_chunk(chunk_id)?;
        read_chunk_at(&mut self.file, chunk_offset)
    }

    fn commit_chunk(&mut self, chunk: &Chunk)
    -> Result<(), Error> {
        let chunk_offset = match self.find_offset_for_chunk(chunk.id) {
            Ok(offset) => offset,
            Err(Error::NotFound(e)) => { todo!(); 0 as u64 },
            Err(e) => Err(e)?,
        };
        write_chunk_at(&mut self.file, chunk, chunk_offset)
    }

    pub fn create(mut file: File, schema: IndexSchema)
    -> Result<Self, Error> {
        validate_schema(&schema)?;

        let mut metadata: Metadata = Metadata::new();
        metadata.next_chunk_id = 3;
        metadata.schema = MessageField::some(schema.clone());
        {
            let mut metadata_chunk = Chunk::new();
            metadata_chunk.id = 0;
            metadata_chunk.set_metadata(metadata.clone());

            write_chunk_at(&mut file, &metadata_chunk, 0)?;
        }
        {
            let mut directory_chunk = Chunk::new();
            directory_chunk.id = 1;
            let directory: &mut Directory = directory_chunk.mut_directory();

            let mut root_node_entry = directory::Entry::new();
            root_node_entry.id = 2;
            root_node_entry.offset = 2;
            directory.entries.push(root_node_entry);

            write_chunk_at(&mut file, &directory_chunk, 1)?;
        }
        {
            let mut root_node_chunk = Chunk::new();
            root_node_chunk.id = 2;
            let root_node: &mut RowDataNode = root_node_chunk.mut_row_data();

            write_chunk_at(&mut file, &root_node_chunk, 2)?;
        }

        Ok(Self {
            file: file,
            metadata: metadata,
        })
    }

    pub fn open(mut file: File)
    -> Result<Self, Error> {
        let mut chunk: Chunk = read_chunk_at(&mut file, 0)?;
        if !chunk.has_metadata() {
            return Err(Error::InvalidArgument("First chunk not metadata as expected!".into()));
        }
        let metadata = chunk.take_metadata();
        if metadata.schema.is_none() {
            return Err(Error::InvalidArgument("No schema present!".into()));
        }
        Ok(Self {
            file: file,
            metadata: metadata,
        })
    }

    pub fn insert(&mut self, op: Insert)
    -> Result<(), Error> {
        // TODO: convert op value into this.
        let row = InternalRow::new();
        let mut root = self.find_chunk(self.metadata.root_chunk_id)?;
        assert!(root.has_row_data());

        if root.compute_size() + row.compute_size() > CHUNK_SIZE as u64 {
            let mut root_copy = root.clone();
            root_copy.id = self.next_chunk_id();

            let mut root_ref = row_data_node::Value::new();
            root_ref.set_child_id(root_copy.id);

            root.mut_row_data().values.clear();
            root.mut_row_data().values.push(root_ref);

            self.split_child(root.mut_row_data(), root_copy.mut_row_data(), 0)?;
        }

        self.insert_non_full(root.mut_row_data(), row)?;

        // TODO: commit stuff.

        Ok(())
    }

    fn insert_non_full(&mut self, node: &mut RowDataNode, row: InternalRow)
    -> Result<(), Error> {
        // find the first value less than row
        let mut i = node.values.len();
        while 0 <= i {
            let value: &row_data_node::Value = &node.values[i];
            if value.has_row_node() && value.row_node().key <= row.key {
                break;
            }
            i -= 1;
        }

        // non-leaf nodes will alternate ref / row values.
        let is_leaf_node = !node.values[i + 1].has_child_id();
        if is_leaf_node {
            let mut row_val = row_data_node::Value::new();
            row_val.set_row_node(row);
            node.values.insert(i, row_val);
            return Ok(());
        } else {
            let child_idx = i + 1;
            let mut child_chunk = self.find_chunk(node.values[child_idx].child_id())?;
            assert!(child_chunk.has_row_data());

            if child_chunk.compute_size() + row.compute_size() > CHUNK_SIZE as u64 {
                // [.., ref, ..] --> [.., ref < val, val, ref > val, .. ]
                self.split_child(node, child_chunk.mut_row_data(), child_idx)?;

                // need to determine which split (left, right) to place the value in.
                let split_val = &node.values[child_idx + 1];
                assert!(split_val.has_row_node());
                if split_val.row_node().key < row.key {
                    child_chunk = self.find_chunk(node.values[child_idx + 2].child_id())?;
                    assert!(child_chunk.has_row_data());
                }
            }

            self.insert_non_full(child_chunk.mut_row_data(), row)?;
            return Ok(());
        }
    }

    // splits a child node within a given parent node in half.
    // the split / middle value is brought up to the parent node.
    fn split_child(&mut self, parent: &mut RowDataNode, left_child: &mut RowDataNode, child_chunk_idx: usize)
    -> Result<(), Error> {
        let mut split_idx = left_child.values.len() / 2;
        if left_child.values[split_idx].has_child_id() {
            split_idx += 1;
            assert!(split_idx < left_child.values.len());
        }
        assert!(left_child.values[split_idx].has_row_node());

        let mut right_child_chunk = Chunk::new();
        right_child_chunk.id = self.next_chunk_id();
        let mut right_child = right_child_chunk.mut_row_data();
        for i in split_idx + 1 .. left_child.values.len() {
            right_child.values.push(left_child.values.remove(split_idx + 1));
        }
        let mut right_child_ref = row_data_node::Value::new();
        right_child_ref.set_child_id(right_child_chunk.id);

        parent.values.insert(child_chunk_idx + 1, right_child_ref);
        parent.values.insert(child_chunk_idx + 1, left_child.values.remove(split_idx));

        Ok(())
    }
}
