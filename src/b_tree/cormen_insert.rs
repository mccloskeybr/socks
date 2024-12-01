use std::io::{Read, Write, Seek};
use protobuf::Message;
use protobuf::MessageField;
use crate::index::*;
use crate::error::*;
use crate::parse::*;
use crate::file::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::schema::*;
use crate::protos::generated::chunk::*;

fn insert_non_full<F: Read + Write + Seek>(
    index: &mut Index<F>, node_chunk: &mut ChunkProto, row: InternalRowProto)
-> Result<(), Error> {
    debug_assert!(node_chunk.has_data());
    debug_assert!(!chunk::would_chunk_overflow(node_chunk, &row));
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
        row_data::commit_chunk(index, node_chunk)?;
        return Ok(());
    } else {
        let child_idx = insert_pos;
        let mut child_chunk = row_data::find_chunk(index, node.values[child_idx].child_id())?;
        debug_assert!(child_chunk.has_data());

        if chunk::would_chunk_overflow(&child_chunk, &row) {
            // [.., ref, ..] --> [.., ref < val, val, ref > val, .. ]
            split_child(index, node_chunk, &mut child_chunk, child_idx)?;

            // need to determine which split (left, right) to place the value in.
            let node: &DataProto = node_chunk.data();
            let split_val = &node.values[child_idx + 1];
            debug_assert!(split_val.has_row_node());
            if split_val.row_node().key < row.key {
                child_chunk = row_data::find_chunk(index, node.values[child_idx + 2].child_id())?;
                debug_assert!(child_chunk.has_data());
            }
        }

        insert_non_full(index, &mut child_chunk, row)?;
        Ok(())
    }
}

// splits a child node within a given parent node in half.
// the split / middle value is brought up to the parent node.
fn split_child<F: Read + Write + Seek>(
    index: &mut Index<F>, parent_chunk: &mut ChunkProto, left_child_chunk: &mut ChunkProto, child_chunk_idx: usize)
-> Result<(), Error> {
    debug_assert!(parent_chunk.has_data());
    debug_assert!(left_child_chunk.has_data());
    let parent: &mut DataProto = parent_chunk.mut_data();
    let left_child: &mut DataProto = left_child_chunk.mut_data();

    let mut split_idx = left_child.values.len() / 2;
    if left_child.values[split_idx].has_child_id() {
        split_idx += 1;
        debug_assert!(split_idx < left_child.values.len());
    }
    debug_assert!(left_child.values[split_idx].has_row_node());

    let mut right_child_chunk = ChunkProto::new();
    let mut right_child = right_child_chunk.mut_data();
    right_child.id = metadata::next_chunk_id(index);
    for i in split_idx + 1 .. left_child.values.len() {
        right_child.values.push(left_child.values.remove(split_idx + 1));
    }
    let mut right_child_ref = data_proto::Value::new();
    right_child_ref.set_child_id(right_child.id);

    parent.values.insert(child_chunk_idx + 1, right_child_ref);
    parent.values.insert(child_chunk_idx + 1, left_child.values.remove(split_idx));

    row_data::commit_chunk(index, left_child_chunk)?;
    row_data::commit_chunk(index, &right_child_chunk)?;
    row_data::commit_chunk(index, parent_chunk)?;

    Ok(())
}

// NOTE: https://www.geeksforgeeks.org/insert-operation-in-b-tree/
// TODO: ensure key doesn't already exist
pub fn insert<F: Read + Write + Seek>(
    index: &mut Index<F>, row: InternalRowProto)
-> Result<(), Error> {
    let mut root_chunk = row_data::find_chunk(index, index.metadata.root_chunk_id)?;
    debug_assert!(root_chunk.has_data());

    if chunk::would_chunk_overflow(&root_chunk, &row) {
        log::trace!("Root overflow detected.");

        // TODO: this is inefficient.
        let mut child_chunk = root_chunk.clone();
        child_chunk.mut_data().id = metadata::next_chunk_id(index);

        let mut child_ref = data_proto::Value::new();
        child_ref.set_child_id(child_chunk.data().id);

        root_chunk.mut_data().values.clear();
        root_chunk.mut_data().values.push(child_ref);

        row_data::commit_chunk(index, &child_chunk)?;
        row_data::commit_chunk(index, &root_chunk)?;

        split_child(index, &mut root_chunk, &mut child_chunk, 0)?;
    }
    insert_non_full(index, &mut root_chunk, row)?;
    metadata::commit_metadata(index)?;
    Ok(())
}
