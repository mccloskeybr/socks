use crate::bp_tree;
use crate::cache;
use crate::chunk;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::table::*;
use crate::LANE_WIDTH;
use protobuf::Message;
use protobuf::MessageField;

fn insert_non_full_leaf<F: Filelike>(
    table: &mut Table<F>,
    node: &mut NodeProto,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    debug_assert!(node.has_leaf());
    let leaf: &mut LeafNodeProto = node.mut_leaf();
    let idx = bp_tree::find_row_idx_for_key(leaf, key);

    leaf.keys.insert(idx, key);
    leaf.rows.insert(idx, row);
    cache::write(table, &node)?;
    Ok(())
}

fn insert_non_full_internal<F: Filelike>(
    table: &mut Table<F>,
    node: &mut NodeProto,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    debug_assert!(node.has_internal());

    let idx = bp_tree::find_next_node_idx_for_key(node.internal(), key)?;
    debug_assert!(idx < node.internal().child_offsets.len());
    let mut child_node = cache::read(table, node.internal().child_offsets[idx])?;
    match &child_node.node_type {
        Some(node_proto::Node_type::Internal(_)) => {
            if chunk::would_chunk_overflow(
                &table.metadata.config,
                child_node.compute_size() as usize + std::mem::size_of::<i32>(),
            ) {
                let right_child = split_child_internal(table, node, &mut child_node, idx)?;
                if node.internal().keys[idx] < key {
                    child_node = right_child;
                }
            }
            return insert_non_full_internal(table, &mut child_node, key, row);
        }
        Some(node_proto::Node_type::Leaf(_)) => {
            if chunk::would_chunk_overflow(
                &table.metadata.config,
                child_node.compute_size() as usize + row.compute_size() as usize,
            ) {
                let right_child = split_child_leaf(table, node, &mut child_node, idx)?;
                if node.internal().keys[idx] < key {
                    child_node = right_child;
                }
            }
            return insert_non_full_leaf(table, &mut child_node, key, row);
        }
        None => unreachable!(),
    }
}

fn split_child_leaf<F: Filelike>(
    table: &mut Table<F>,
    parent: &mut NodeProto,
    child: &mut NodeProto,
    child_chunk_idx: usize,
) -> Result<NodeProto, Error> {
    log::trace!("Splitting leaf node.");
    debug_assert!(parent.has_internal());
    debug_assert!(child.has_leaf());
    let mut split_idx = child.leaf().keys.len() / 2;

    let left_child = child;
    let mut right_child = NodeProto::new();
    right_child.offset = next_chunk_offset(table);
    right_child.parent_offset = parent.offset;
    right_child.left_sibling_offset = left_child.offset;
    right_child.mut_leaf().keys = left_child.mut_leaf().keys.split_off(split_idx);
    right_child.mut_leaf().rows = left_child.mut_leaf().rows.split_off(split_idx);

    left_child.right_sibling_offset = right_child.offset;

    parent
        .mut_internal()
        .keys
        .insert(child_chunk_idx, right_child.leaf().keys[0]);
    parent
        .mut_internal()
        .child_offsets
        .insert(child_chunk_idx + 1, right_child.offset);

    cache::write(table, left_child)?;
    cache::write(table, &right_child)?;
    cache::write(table, parent)?;

    Ok(right_child)
}

fn split_child_internal<F: Filelike>(
    table: &mut Table<F>,
    parent: &mut NodeProto,
    child: &mut NodeProto,
    child_chunk_idx: usize,
) -> Result<NodeProto, Error> {
    log::trace!("Splitting internal node.");
    debug_assert!(parent.has_internal());
    debug_assert!(child.has_internal());
    let mut split_idx = child.internal().keys.len() / 2;

    let left_child = child;
    let mut right_child = NodeProto::new();
    right_child.offset = next_chunk_offset(table);
    right_child.parent_offset = parent.offset;
    right_child.left_sibling_offset = left_child.offset;
    right_child.mut_internal().keys = left_child.mut_internal().keys.split_off(split_idx);
    right_child.mut_internal().child_offsets =
        left_child.mut_internal().child_offsets.split_off(split_idx);

    left_child.right_sibling_offset = right_child.offset;

    parent.mut_internal().keys.insert(
        child_chunk_idx,
        left_child.internal().keys[left_child.internal().keys.len() - 1],
    );
    parent
        .mut_internal()
        .child_offsets
        .insert(child_chunk_idx + 1, right_child.offset);

    cache::write(table, left_child)?;
    cache::write(table, &right_child)?;
    cache::write(table, parent)?;

    Ok(right_child)
}

// NOTE: https://www.geeksforgeeks.org/insertion-in-a-b-tree/
// TODO: ensure key doesn't already exist
pub fn insert<F: Filelike>(
    table: &mut Table<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    let mut root_node = cache::read(table, table.metadata.root_chunk_offset)?;
    debug_assert!(root_node.has_internal());

    if root_node.internal().keys.len() + root_node.internal().child_offsets.len() == 0 {
        log::trace!("Inserting first value.");

        let mut child_node = NodeProto::new();
        child_node.offset = next_chunk_offset(table);
        child_node.parent_offset = root_node.offset;
        child_node.mut_leaf().keys.push(key);
        child_node.mut_leaf().rows.push(row);

        root_node
            .mut_internal()
            .child_offsets
            .push(child_node.offset);

        cache::write(table, &child_node)?;
        cache::write(table, &root_node)?;

        commit_metadata(table)?;
        return Ok(());
    }

    if chunk::would_chunk_overflow(
        &table.metadata.config,
        root_node.compute_size() as usize + std::mem::size_of::<i32>(),
    ) {
        log::trace!("Root overflow detected.");

        // TODO: this is inefficient.
        let mut child_node = root_node.clone();
        child_node.offset = next_chunk_offset(table);

        root_node.mut_internal().keys.clear();
        root_node.mut_internal().child_offsets.clear();
        root_node
            .mut_internal()
            .child_offsets
            .push(child_node.offset);

        split_child_internal(table, &mut root_node, &mut child_node, 0)?;
    }
    insert_non_full_internal(table, &mut root_node, key, row)?;
    commit_metadata(table)?;
    Ok(())
}
