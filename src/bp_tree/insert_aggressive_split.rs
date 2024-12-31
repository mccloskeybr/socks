use crate::bp_tree;
use crate::buffer::Buffer;
use crate::buffer_pool::ShardedBufferPool;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::*;
use protobuf::Message;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

// NOTE: Expects node to be non-full.
async fn insert_leaf<F: Filelike>(
    table: &mut Table<F>,
    buffer_pool: &mut ShardedBufferPool,
    node: &mut Buffer,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    debug_assert!(node.has_leaf());
    let leaf: &mut LeafNodeProto = node.mut_leaf();
    let idx = bp_tree::find_row_idx_for_key(leaf, key);
    leaf.keys.insert(idx, key);
    leaf.rows.insert(idx, row);
    chunk::write_chunk_at(table.file, node, node.offset).await?;
    Ok(())
}

// NOTE: Expects node to be non-full.
async fn insert_internal<F: Filelike>(
    table: &mut Table<F>,
    buffer_pool: &mut ShardedBufferPool,
    node: MutexGuard<NodeProto>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    let idx = bp_tree::find_next_node_idx_for_key(node.internal(), key)?;
    let mut child_mutex: Arc<Mutex<NodeProto>> = buffer_pool
        .read_from_table(table, node.internal().child_offsets[idx])
        .await?;
    let mut child_buffer = child.lock().await;
    let mut child = child_buffer.get_mut();
    match &child.node_type {
        Some(node_proto::Node_type::Internal(_)) => {
            if child.would_overflow(std::mem::size_of::<i32>()) {
                let right_child_buffer =
                    split_child_internal(table, buffer_pool, node, &mut child, idx).await?;
                if node.internal().keys[idx] < key {
                    child_buffer = right_child_buffer;
                }
            }
            drop(node);
            return Box::pin(insert_internal(table, buffer_pool, child, key, row)).await;
        }
        Some(node_proto::Node_type::Leaf(_)) => {
            if child.would_overflow(row.compute_size() as usize) {
                let right_child_buffer =
                    split_child_leaf(table, buffer_pool, node, &mut child, idx).await?;
                if node.internal().keys[idx] < key {
                    child_buffer = right_child_buffer;
                }
            }
            drop(node);
            return insert_leaf(table, buffer_pool, child, key, row).await;
        }
        None => unreachable!(),
    }
}

async fn split_child_leaf<F: Filelike>(
    table: &mut Table<F>,
    buffer_pool: &mut ShardedBufferPool,
    parent: &mut Buffer,
    child: &mut Buffer,
    child_chunk_idx: usize,
) -> Result<MutexGuard<Buffer>, Error> {
    log::trace!("Splitting leaf node.");
    debug_assert!(parent.has_internal());
    debug_assert!(child.has_leaf());
    let split_idx = child.get().leaf().keys.len() / 2;

    let parent = parent.get_mut();
    let left_child = child.get_mut();
    let mut right_child_mutex = buffer_pool.new_for_table(table);
    let mut right_child_buffer = right_child.lock().await;
    let right_child = right_child_lock.get_mut();
    right_child.offset = table.next_chunk_offset();
    right_child.parent_offset = parent.offset;
    right_child.mut_leaf().keys = left_child.mut_leaf().keys.split_off(split_idx);
    right_child.mut_leaf().rows = left_child.mut_leaf().rows.split_off(split_idx);

    parent
        .mut_internal()
        .keys
        .insert(child_chunk_idx, right_child.leaf().keys[0]);
    parent
        .mut_internal()
        .child_offsets
        .insert(child_chunk_idx + 1, right_child.offset);

    Ok(right_child_buffer)
}

async fn split_child_internal<F: Filelike>(
    table: &mut Table<F>,
    buffer_pool: &mut ShardedBufferPool,
    parent: &mut MutexGuard<Buffer>,
    child: &mut MutexGuard<Buffer>,
    child_chunk_idx: usize,
) -> Result<MutexGuard<Buffer>, Error> {
    log::trace!("Splitting internal node.");
    debug_assert!(parent.has_internal());
    debug_assert!(child.has_internal());
    let split_idx = child.internal().keys.len() / 2;

    let parent = parent.get_mut();
    let left_child = child.get_mut();
    let mut right_child_mutex = buffer_pool.new_for_table(table);
    let mut right_child_buffer = right_child_mutex.lock().await;
    let mut right_child = right_child_buffer.get_mut();
    right_child.offset = table.next_chunk_offset();
    right_child.parent_offset = parent.offset;
    right_child.mut_internal().keys = left_child.mut_internal().keys.split_off(split_idx);
    right_child.mut_internal().child_offsets =
        left_child.mut_internal().child_offsets.split_off(split_idx);

    parent.mut_internal().keys.insert(
        child_chunk_idx,
        left_child.internal().keys[left_child.internal().keys.len() - 1],
    );
    parent
        .mut_internal()
        .child_offsets
        .insert(child_chunk_idx + 1, right_child.offset);

    Ok(right_child_buffer)
}

// NOTE: https://www.geeksforgeeks.org/insertion-in-a-b-tree/
// TODO: ensure key doesn't already exist
pub(crate) async fn insert<F: Filelike>(
    table: &mut Table<F>,
    buffer_pool: &mut ShardedBufferPool,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    let mut root_node_mutex = buffer_pool
        .read_from_table(table, table.metadata.root_chunk_offset)
        .await?;
    let mut root_node = root_node_mutex.lock().await;
    debug_assert!(root_node.has_internal());

    if root_node.internal().keys.len() + root_node.internal().child_offsets.len() == 0 {
        log::trace!("Inserting first value.");

        let mut child_mutex = buffer_pool.new_for_table(table);
        let mut child_buffer = child_mutex.lock().await;
        let child = child_buffer.get_mut();
        child.offset = table.next_chunk_offset();
        child.parent_offset = root_node.offset;
        child.mut_leaf().keys.push(key);
        child.mut_leaf().rows.push(row);

        root_node.mut_internal().child_offsets.push(child.offset);

        table.commit_metadata().await?;
        return Ok(());
    }

    if chunk::would_chunk_overflow(root_node.compute_size() as usize + std::mem::size_of::<i32>()) {
        log::trace!("Root overflow detected.");
        let mut child = root_node.clone();
        child.offset = table.next_chunk_offset();
        chunk::write_chunk_at(table.file, child, child.offset).await?;

        root_node.mut_internal().keys.clear();
        root_node.mut_internal().child_offsets.clear();
        root_node.mut_internal().child_offsets.push(child.offset);

        split_child_internal(table, buffer_pool, &mut root_node, &mut child, 0).await?;
    }
    drop(root_node);
    insert_internal(table, buffer_pool, &mut root_node_mutex, key, row).await?;
    table.commit_metadata().await?;
    Ok(())
}
