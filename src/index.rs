#[cfg(test)]
#[path = "./index_test.rs"]
mod test;

use std::io::{Read, Write, Seek};
use protobuf::Message;
use protobuf::MessageField;
use crate::chunk::*;
use crate::error::*;
use crate::transform::*;
use crate::validations::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::schema::*;
use crate::protos::generated::chunk::*;

// Index file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub struct Index<'a, F: 'a + Read + Write + Seek> {
    file: &'a mut F,
    metadata: Metadata,
    num_chunk_writes: u64,
    num_chunk_reads: u64,
}

impl<'a, F: 'a + Read + Write + Seek> Index<'a, F> {
    fn next_chunk_id(&mut self) -> u32 {
        let id = self.metadata.next_chunk_id;
        self.metadata.next_chunk_id += 1;
        id
    }

    fn commit_metadata(&mut self)
    -> Result<(), Error> {
        log::trace!("Committing metadata.");
        let mut metadata_chunk = Chunk::new();
        metadata_chunk.set_metadata(self.metadata.clone());
        write_chunk_at::<F>(&mut self.file, &metadata_chunk, 0)?;
        self.num_chunk_writes += 1;
        Ok(())
    }

    fn find_offset_for_chunk(&mut self, chunk_id: u32)
    -> Result<u32, Error> {
        for i in 0..self.metadata.num_directories {
            let dir_chunk_offset = i + 1;
            let mut dir_chunk = read_chunk_at::<F>(&mut self.file, dir_chunk_offset)?;
            self.num_chunk_reads += 1;
            if !dir_chunk.has_directory() {
                return Err(Error::Internal(format!(
                            "Chunk with offset: {} is not a directory!",
                            dir_chunk_offset).into()));
            }
            let directory = dir_chunk.take_directory();
            for entry in directory.entries {
                if entry.id == chunk_id {
                    return Ok(entry.offset);
                }
            }
        }
        Err(Error::NotFound(format!(
                    "Chunk {} not found!",
                    chunk_id).into()))
    }

    fn update_chunk_offset(&mut self, chunk_id: u32, chunk_offset: u32)
    -> Result<(), Error> {
        for i in 0..self.metadata.num_directories {
            let dir_chunk_offset = i + 1;
            let mut dir_chunk = read_chunk_at::<F>(&mut self.file, dir_chunk_offset)?;
            self.num_chunk_reads += 1;
            if !dir_chunk.has_directory() {
                return Err(Error::Internal(format!(
                            "Chunk with offset: {} is not a directory!",
                            dir_chunk_offset).into()));
            }
            let directory = dir_chunk.mut_directory();
            for entry in &mut directory.entries {
                if entry.id == chunk_id {
                    entry.offset = chunk_offset;
                    write_chunk_at::<F>(&mut self.file, &dir_chunk, dir_chunk_offset)?;
                    self.num_chunk_writes += 1;
                    return Ok(());
                }
            }
        }
        Err(Error::NotFound(format!(
                    "Chunk {} not found!",
                    chunk_id).into()))
    }

    fn find_and_read_chunk(&mut self, chunk_id: u32)
    -> Result<Chunk, Error> {
        let chunk_offset = self.find_offset_for_chunk(chunk_id)?;
        self.num_chunk_reads += 1;
        read_chunk_at::<F>(&mut self.file, chunk_offset)
    }

    fn commit_chunk(&mut self, chunk: &Chunk)
    -> Result<(), Error> {
        assert!(chunk.has_row_data());
        log::trace!("Committing chunk: {}", chunk.row_data().id);
        match self.find_offset_for_chunk(chunk.row_data().id) {
            // NOTE: found existing chunk.
            Ok(chunk_offset) => {
                write_chunk_at::<F>(&mut self.file, chunk, chunk_offset)?;
                self.num_chunk_writes += 1;
            }
            // NOTE: detected writing a new chunk.
            // TODO: consider write partial success.
            Err(Error::NotFound(..)) => {
                log::trace!("Chunk not found: {}, creating a new one.", chunk.row_data().id);

                // write chunk
                let chunk_offset = self.metadata.next_chunk_offset;
                write_chunk_at::<F>(&mut self.file, chunk, chunk_offset)?;
                self.num_chunk_writes += 1;
                self.metadata.next_chunk_offset += 1;

                // write mapping
                let mut dir_entry = directory::Entry::new();
                dir_entry.id = chunk.row_data().id;
                dir_entry.offset = chunk_offset;

                let mut dir_chunk: Option<(Chunk, u32)> = None;
                for i in 0..self.metadata.num_directories {
                    let dir_chunk_offset = i + 1;
                    let mut test_dir_chunk = read_chunk_at::<F>(&mut self.file, dir_chunk_offset)?;
                    self.num_chunk_reads += 1;
                    if !test_dir_chunk.has_directory() {
                        return Err(Error::Internal(format!(
                                    "Chunk with offset: {} is not a directory!",
                                    dir_chunk_offset).into()));
                    }
                    if !would_chunk_overflow(&test_dir_chunk, &dir_entry) {
                        dir_chunk = Some((test_dir_chunk, dir_chunk_offset));
                        break;
                    }
                }

                if let Some(mut dir_chunk) = dir_chunk {
                    log::trace!("Found a directory with existing space: {}", dir_chunk.1);
                    dir_chunk.0.mut_directory().entries.push(dir_entry);
                    log::trace!("{} :: {}", dir_chunk.0, dir_chunk.1);
                    write_chunk_at::<F>(&mut self.file, &dir_chunk.0, dir_chunk.1)?;
                    self.num_chunk_writes += 1;
                } else {
                    log::trace!("No directory with space available, creating a new one.");

                    let mut new_dir_chunk = Chunk::new();
                    new_dir_chunk.mut_directory().entries.push(dir_entry);

                    let new_dir_offset = 1 + self.metadata.num_directories;
                    let swap_chunk_offset = self.metadata.next_chunk_offset;
                    let swap_chunk = read_chunk_at::<F>(&mut self.file, new_dir_offset)?;
                    self.num_chunk_reads += 1;
                    assert!(swap_chunk.has_row_data());
                    write_chunk_at::<F>(&mut self.file, &swap_chunk, swap_chunk_offset)?;
                    self.num_chunk_writes += 1;
                    self.metadata.next_chunk_offset += 1;
                    self.update_chunk_offset(swap_chunk.row_data().id, swap_chunk_offset)?;

                    write_chunk_at::<F>(&mut self.file, &new_dir_chunk, new_dir_offset)?;
                    self.num_chunk_writes += 1;
                    self.metadata.num_directories += 1;
                }

                self.commit_metadata()?;
            },
            Err(e) => { Err(e)? },
        };
        Ok(())
    }

    pub fn create(file: &'a mut F, schema: IndexSchema)
    -> Result<Self, Error> {
        validate_schema(&schema)?;

        let mut metadata: Metadata = Metadata::new();
        metadata.schema = MessageField::some(schema.clone());
        metadata.next_chunk_id = 1;
        metadata.next_chunk_offset = 3;
        metadata.root_chunk_id = 0;
        metadata.num_directories = 1;
        {
            let mut metadata_chunk = Chunk::new();
            metadata_chunk.set_metadata(metadata.clone());
            write_chunk_at::<F>(file, &metadata_chunk, 0)?;
        }
        {
            let mut root_node_entry = directory::Entry::new();
            root_node_entry.id = 0;
            root_node_entry.offset = 2;
            let mut directory_chunk = Chunk::new();
            let directory: &mut Directory = directory_chunk.mut_directory();
            directory.entries.push(root_node_entry);
            write_chunk_at::<F>(file, &directory_chunk, 1)?;
        }
        {
            let mut root_node_chunk = Chunk::new();
            let root_node: &mut RowDataNode = root_node_chunk.mut_row_data();
            root_node.id = 0;
            write_chunk_at::<F>(file, &root_node_chunk, 2)?;
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
        let mut chunk: Chunk = read_chunk_at::<F>(file, 0)?;
        if !chunk.has_metadata() {
            return Err(Error::InvalidArgument(
                    "First chunk not metadata as expected!".into()));
        }
        let metadata = chunk.take_metadata();
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
        let mut root_chunk = self.find_and_read_chunk(self.metadata.root_chunk_id)?;
        assert!(root_chunk.has_row_data());
        if would_chunk_overflow(&root_chunk, &row) {
            log::trace!("Root overflow detected.");

            // TODO: this is inefficient.
            let mut child_chunk = root_chunk.clone();
            child_chunk.mut_row_data().id = self.next_chunk_id();

            let mut child_ref = row_data_node::Value::new();
            child_ref.set_child_id(child_chunk.row_data().id);

            root_chunk.mut_row_data().values.clear();
            root_chunk.mut_row_data().values.push(child_ref);

            self.commit_chunk(&child_chunk)?;
            self.commit_chunk(&root_chunk)?;

            self.split_child(&mut root_chunk, &mut child_chunk, 0)?;
        }
        self.insert_non_full(&mut root_chunk, row)?;
        Ok(())
    }

    fn insert_non_full(&mut self, node_chunk: &mut Chunk, row: InternalRow)
    -> Result<(), Error> {
        assert!(node_chunk.has_row_data());
        assert!(!would_chunk_overflow(node_chunk, &row));
        let node: &mut RowDataNode = node_chunk.mut_row_data();

        // find the first value less than row
        let mut insert_pos = 0;
        if !node.values.is_empty() {
            for i in (0..node.values.len()).rev() {
                let value: &row_data_node::Value = &node.values[i];
                if value.has_row_node() && value.row_node().key <= row.key {
                    insert_pos = i + 1;
                    break;
                }
            }
        }

        // NOTE: non-leaf nodes will alternate ref / row values.
        // TODO: is this true? can this be out of bounds?
        if is_leaf_node(node) {
            log::trace!("Inserting row in leaf node: {}.", node.id);
            let mut row_val = row_data_node::Value::new();
            row_val.set_row_node(row);
            node_chunk.mut_row_data().values.insert(insert_pos, row_val);
            self.commit_chunk(node_chunk)?;
            return Ok(());
        } else {
            let child_idx = insert_pos;
            let mut child_chunk = self.find_and_read_chunk(node.values[child_idx].child_id())?;
            assert!(child_chunk.has_row_data());

            if would_chunk_overflow(&child_chunk, &row) {
                // [.., ref, ..] --> [.., ref < val, val, ref > val, .. ]
                self.split_child(node_chunk, &mut child_chunk, child_idx)?;

                // need to determine which split (left, right) to place the value in.
                let node: &RowDataNode = node_chunk.row_data();
                let split_val = &node.values[child_idx + 1];
                assert!(split_val.has_row_node());
                if split_val.row_node().key < row.key {
                    child_chunk = self.find_and_read_chunk(node.values[child_idx + 2].child_id())?;
                    assert!(child_chunk.has_row_data());
                }
            }

            self.insert_non_full(&mut child_chunk, row)?;
            Ok(())
        }
    }

    // splits a child node within a given parent node in half.
    // the split / middle value is brought up to the parent node.
    fn split_child(&mut self, parent_chunk: &mut Chunk, left_child_chunk: &mut Chunk, child_chunk_idx: usize)
    -> Result<(), Error> {
        assert!(parent_chunk.has_row_data());
        assert!(left_child_chunk.has_row_data());
        let parent: &mut RowDataNode = parent_chunk.mut_row_data();
        let left_child: &mut RowDataNode = left_child_chunk.mut_row_data();

        let mut split_idx = left_child.values.len() / 2;
        if left_child.values[split_idx].has_child_id() {
            split_idx += 1;
            assert!(split_idx < left_child.values.len());
        }
        assert!(left_child.values[split_idx].has_row_node());

        let mut right_child_chunk = Chunk::new();
        let mut right_child = right_child_chunk.mut_row_data();
        right_child.id = self.next_chunk_id();
        for i in split_idx + 1 .. left_child.values.len() {
            right_child.values.push(left_child.values.remove(split_idx + 1));
        }
        let mut right_child_ref = row_data_node::Value::new();
        right_child_ref.set_child_id(right_child.id);

        parent.values.insert(child_chunk_idx + 1, right_child_ref);
        parent.values.insert(child_chunk_idx + 1, left_child.values.remove(split_idx));

        self.commit_chunk(left_child_chunk)?;
        self.commit_chunk(&right_child_chunk)?;
        self.commit_chunk(parent_chunk)?;
        self.commit_metadata()?;

        Ok(())
    }
}
