use std::io::{Read, Write, Seek};
use protobuf::Message;
use protobuf::MessageField;
use crate::index::*;
use crate::error::*;
use crate::parse::*;
use crate::file::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::config::*;
use crate::protos::generated::chunk::*;

fn insert_non_full_leaf<F: Read + Write + Seek>(
    index: &mut Index<F>, node_chunk: &mut ChunkProto, row: InternalRowProto)
-> Result<(), Error> {
    debug_assert!(node_chunk.has_node());
    debug_assert!(node_chunk.node().has_leaf());
    let leaf: &mut LeafNodeProto = node_chunk.mut_node().mut_leaf();
    let mut idx = leaf.rows.len() - 1;
    while row.key < leaf.rows[idx].key { idx -= 1; }
    leaf.rows.insert(idx + 1, row);
    row_data::commit_chunk(index, &node_chunk)?;
    Ok(())
}

fn insert_non_full_internal<F: Read + Write + Seek>(
    index: &mut Index<F>, node_chunk: &mut ChunkProto, row: InternalRowProto)
-> Result<(), Error> {
    debug_assert!(node_chunk.has_node());
    debug_assert!(node_chunk.node().has_internal());

    // find child node row should move into
    let mut idx = node_chunk.node().internal().values.len() - 1;
    while idx > 0 && (
            !node_chunk.node().internal().values[idx].has_key() ||
            row.key < node_chunk.node().internal().values[idx].key().to_string()) {
        idx -= 1;
    }
    idx += node_chunk.node().internal().values[idx].has_key() as usize;

    debug_assert!(node_chunk.node().internal().values[idx].has_child_id());
    let mut child_chunk = row_data::find_chunk(index, node_chunk.node().internal().values[idx].child_id())?;
    debug_assert!(child_chunk.has_node());
    match &child_chunk.mut_node().node_type {
        Some(node_proto::Node_type::Internal(_)) => {
            if chunk::would_chunk_overflow(&index.db_config.file,
                                           child_chunk.compute_size() as usize + row.key.len() + std::mem::size_of::<i32>()) {
                let right_child = split_child_internal(index, node_chunk, &mut child_chunk, idx)?;
                if node_chunk.node().internal().values[idx + 1].key().to_string() < row.key {
                    child_chunk = right_child;
                }
            }
            return insert_non_full_internal(index, &mut child_chunk, row);
        },
        Some(node_proto::Node_type::Leaf(_)) => {
            if chunk::would_chunk_overflow(&index.db_config.file,
                                           child_chunk.compute_size() as usize + row.compute_size() as usize) {
                let right_child = split_child_leaf(index, node_chunk, &mut child_chunk, idx)?;
                if node_chunk.node().internal().values[idx + 1].key().to_string() < row.key {
                    child_chunk = right_child;
                }
            }
            return insert_non_full_leaf(index, &mut child_chunk, row);
        },
        None => unreachable!(),
    }
}

fn split_child_leaf<F: Read + Write + Seek>(
    index: &mut Index<F>, parent_chunk: &mut ChunkProto, left_child_chunk: &mut ChunkProto,
    child_chunk_idx: usize)
-> Result<ChunkProto, Error> {
    debug_assert!(parent_chunk.node().has_internal());
    debug_assert!(left_child_chunk.node().has_leaf());
    let parent: &mut InternalNodeProto = parent_chunk.mut_node().mut_internal();
    let left_child: &mut LeafNodeProto = left_child_chunk.mut_node().mut_leaf();

    let mut split_idx = left_child.rows.len() / 2;

    let mut right_child_chunk = ChunkProto::new();
    let mut right_child = right_child_chunk.mut_node();
    right_child.id = metadata::next_chunk_id(index);
    right_child.mut_leaf().rows = left_child.rows.split_off(split_idx);

    let mut right_child_ref = internal_node_proto::Value::new();
    right_child_ref.set_child_id(right_child.id.clone());
    parent.values.insert(child_chunk_idx + 1, right_child_ref);

    let mut right_child_smallest = internal_node_proto::Value::new();
    right_child_smallest.set_key(right_child.leaf().rows[0].key.clone());
    parent.values.insert(child_chunk_idx + 1, right_child_smallest);

    row_data::commit_chunk(index, left_child_chunk)?;
    row_data::commit_chunk(index, &right_child_chunk)?;
    row_data::commit_chunk(index, parent_chunk)?;

    Ok(right_child_chunk)
}

fn split_child_internal<F: Read + Write + Seek>(
    index: &mut Index<F>, parent_chunk: &mut ChunkProto, left_child_chunk: &mut ChunkProto,
    child_chunk_idx: usize)
-> Result<ChunkProto, Error> {
    debug_assert!(parent_chunk.node().has_internal());
    debug_assert!(left_child_chunk.node().has_internal());
    let parent: &mut InternalNodeProto = parent_chunk.mut_node().mut_internal();
    let left_child: &mut InternalNodeProto = left_child_chunk.mut_node().mut_internal();

    let mut split_idx = left_child.values.len() / 2;
    split_idx += left_child.values[split_idx].has_child_id() as usize;
    debug_assert!(left_child.values[split_idx].has_key());

    let mut right_child_chunk = ChunkProto::new();
    let mut right_child = right_child_chunk.mut_node();
    right_child.id = metadata::next_chunk_id(index);
    right_child.mut_internal().values = left_child.values.split_off(split_idx + 1);

    let mut right_child_ref = internal_node_proto::Value::new();
    right_child_ref.set_child_id(right_child.id);

    parent.values.insert(child_chunk_idx + 1, right_child_ref);
    parent.values.insert(child_chunk_idx + 1, left_child.values.remove(split_idx));

    row_data::commit_chunk(index, left_child_chunk)?;
    row_data::commit_chunk(index, &right_child_chunk)?;
    row_data::commit_chunk(index, parent_chunk)?;

    Ok(right_child_chunk)
}

// NOTE: https://www.geeksforgeeks.org/insertion-in-a-b-tree/
// TODO: ensure key doesn't already exist
pub fn insert<F: Read + Write + Seek>(
    index: &mut Index<F>, row: InternalRowProto)
-> Result<(), Error> {
    let mut root_chunk = row_data::find_chunk(index, index.metadata.root_chunk_id)?;
    debug_assert!(root_chunk.node().has_internal());

    if root_chunk.node().internal().values.len() == 0 {
        log::trace!("Inserting first value.");

        let mut data_chunk = ChunkProto::new();
        let mut data = data_chunk.mut_node();
        data.id = metadata::next_chunk_id(index);
        data.mut_leaf().rows.push(row.clone());

        let mut data_chunk_ref = internal_node_proto::Value::new();
        data_chunk_ref.set_child_id(data.id);
        root_chunk.mut_node().mut_internal().values.push(data_chunk_ref);

        row_data::commit_chunk(index, &data_chunk)?;
        row_data::commit_chunk(index, &root_chunk)?;

        metadata::commit_metadata(index)?;
        return Ok(());
    }

    if chunk::would_chunk_overflow(&index.db_config.file,
                                root_chunk.compute_size() as usize + row.key.len() + std::mem::size_of::<i32>()) {
        log::trace!("Root overflow detected.");

        // TODO: this is inefficient.
        let mut child_chunk = root_chunk.clone();
        child_chunk.mut_node().id = metadata::next_chunk_id(index);

        let mut child_ref = internal_node_proto::Value::new();
        child_ref.set_child_id(child_chunk.node().id);

        root_chunk.mut_node().mut_internal().values.clear();
        root_chunk.mut_node().mut_internal().values.push(child_ref);

        split_child_internal(index, &mut root_chunk, &mut child_chunk, 0)?;
    }
    insert_non_full_internal(index, &mut root_chunk, row)?;
    metadata::commit_metadata(index)?;
    Ok(())
}
