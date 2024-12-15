use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::table::bp_tree;
use crate::table::cache;
use crate::table::table::*;
use crate::table::*;
use crate::LANE_WIDTH;
use protobuf::Message;
use protobuf::MessageField;
use std::io::{Read, Seek, Write};

fn insert_non_full_leaf<F: Read + Write + Seek>(
    table: &mut Table<F>,
    node_chunk: &mut ChunkProto,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    debug_assert!(node_chunk.has_node());
    debug_assert!(node_chunk.node().has_leaf());

    let leaf: &mut LeafNodeProto = node_chunk.mut_node().mut_leaf();
    let idx = bp_tree::find_row_idx_for_key(&table.metadata.config, leaf, key);

    leaf.keys.insert(idx, key);
    leaf.rows.insert(idx, row);
    cache::write(table, &node_chunk)?;
    Ok(())
}

fn insert_non_full_internal<F: Read + Write + Seek>(
    table: &mut Table<F>,
    node_chunk: &mut ChunkProto,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    debug_assert!(node_chunk.has_node());
    debug_assert!(node_chunk.node().has_internal());

    let idx = bp_tree::find_next_node_idx_for_key(
        &table.metadata.config,
        node_chunk.node().internal(),
        key,
    )?;
    debug_assert!(idx < node_chunk.node().internal().child_offsets.len());
    let mut child_chunk = cache::read(table, node_chunk.node().internal().child_offsets[idx])?;
    debug_assert!(child_chunk.has_node());
    match &child_chunk.mut_node().node_type {
        Some(node_proto::Node_type::Internal(_)) => {
            if chunk::would_chunk_overflow(
                &table.metadata.config,
                child_chunk.compute_size() as usize + std::mem::size_of::<i32>(),
            ) {
                let right_child = split_child_internal(table, node_chunk, &mut child_chunk, idx)?;
                if node_chunk.node().internal().keys[idx] < key {
                    child_chunk = right_child;
                }
            }
            return insert_non_full_internal(table, &mut child_chunk, key, row);
        }
        Some(node_proto::Node_type::Leaf(_)) => {
            if chunk::would_chunk_overflow(
                &table.metadata.config,
                child_chunk.compute_size() as usize + row.compute_size() as usize,
            ) {
                let right_child = split_child_leaf(table, node_chunk, &mut child_chunk, idx)?;
                if node_chunk.node().internal().keys[idx] < key {
                    child_chunk = right_child;
                }
            }
            return insert_non_full_leaf(table, &mut child_chunk, key, row);
        }
        None => unreachable!(),
    }
}

fn split_child_leaf<F: Read + Write + Seek>(
    table: &mut Table<F>,
    parent_chunk: &mut ChunkProto,
    left_child_chunk: &mut ChunkProto,
    child_chunk_idx: usize,
) -> Result<ChunkProto, Error> {
    log::trace!("Splitting leaf node.");
    debug_assert!(parent_chunk.node().has_internal());
    debug_assert!(left_child_chunk.node().has_leaf());
    let parent: &mut NodeProto = parent_chunk.mut_node();
    let left_child: &mut LeafNodeProto = left_child_chunk.mut_node().mut_leaf();

    let mut split_idx = left_child.keys.len() / 2;

    let mut right_child_chunk = ChunkProto::new();
    let mut right_child = right_child_chunk.mut_node();
    right_child.offset = table::next_chunk_offset(table);
    right_child.parent_offset = parent.offset;
    right_child.mut_leaf().keys = left_child.keys.split_off(split_idx);
    right_child.mut_leaf().rows = left_child.rows.split_off(split_idx);

    parent
        .mut_internal()
        .keys
        .insert(child_chunk_idx, right_child.leaf().keys[0]);
    parent
        .mut_internal()
        .child_offsets
        .insert(child_chunk_idx + 1, right_child.offset);

    cache::write(table, left_child_chunk)?;
    cache::write(table, &right_child_chunk)?;
    cache::write(table, parent_chunk)?;

    Ok(right_child_chunk)
}

fn split_child_internal<F: Read + Write + Seek>(
    table: &mut Table<F>,
    parent_chunk: &mut ChunkProto,
    left_child_chunk: &mut ChunkProto,
    child_chunk_idx: usize,
) -> Result<ChunkProto, Error> {
    log::trace!("Splitting internal node.");
    debug_assert!(parent_chunk.node().has_internal());
    debug_assert!(left_child_chunk.node().has_internal());
    let parent: &mut NodeProto = parent_chunk.mut_node();
    let left_child: &mut InternalNodeProto = left_child_chunk.mut_node().mut_internal();

    let mut split_idx = left_child.keys.len() / 2;

    let mut right_child_chunk = ChunkProto::new();
    let mut right_child = right_child_chunk.mut_node();
    right_child.offset = table::next_chunk_offset(table);
    right_child.parent_offset = parent.offset;
    right_child.mut_internal().keys = left_child.keys.split_off(split_idx);
    right_child.mut_internal().child_offsets = left_child.child_offsets.split_off(split_idx);

    parent
        .mut_internal()
        .keys
        .insert(child_chunk_idx, left_child.keys[left_child.keys.len() - 1]);
    parent
        .mut_internal()
        .child_offsets
        .insert(child_chunk_idx + 1, right_child.offset);

    cache::write(table, left_child_chunk)?;
    cache::write(table, &right_child_chunk)?;
    cache::write(table, parent_chunk)?;

    Ok(right_child_chunk)
}

// NOTE: https://www.geeksforgeeks.org/insertion-in-a-b-tree/
// TODO: ensure key doesn't already exist
pub fn insert<F: Read + Write + Seek>(
    table: &mut Table<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    let mut root_chunk = cache::read(table, table.metadata.root_chunk_offset)?;
    debug_assert!(root_chunk.node().has_internal());

    if root_chunk.node().internal().keys.len() + root_chunk.node().internal().child_offsets.len()
        == 0
    {
        log::trace!("Inserting first value.");

        let mut data_chunk = ChunkProto::new();
        let mut data = data_chunk.mut_node();
        data.offset = table::next_chunk_offset(table);
        data.parent_offset = root_chunk.node().offset;
        data.mut_leaf().keys.push(key);
        data.mut_leaf().rows.push(row);

        root_chunk
            .mut_node()
            .mut_internal()
            .child_offsets
            .push(data.offset);

        cache::write(table, &data_chunk)?;
        cache::write(table, &root_chunk)?;

        table::commit_metadata(table)?;
        return Ok(());
    }

    if chunk::would_chunk_overflow(
        &table.metadata.config,
        root_chunk.compute_size() as usize + std::mem::size_of::<i32>(),
    ) {
        log::trace!("Root overflow detected.");

        // TODO: this is inefficient.
        let mut child_chunk = root_chunk.clone();
        child_chunk.mut_node().offset = table::next_chunk_offset(table);

        root_chunk.mut_node().mut_internal().keys.clear();
        root_chunk.mut_node().mut_internal().child_offsets.clear();
        root_chunk
            .mut_node()
            .mut_internal()
            .child_offsets
            .push(child_chunk.node().offset);

        split_child_internal(table, &mut root_chunk, &mut child_chunk, 0)?;
    }
    insert_non_full_internal(table, &mut root_chunk, key, row)?;
    table::commit_metadata(table)?;
    Ok(())
}
