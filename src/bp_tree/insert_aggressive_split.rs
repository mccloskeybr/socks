use crate::bp_tree;
use crate::buffer::Buffer;
use crate::buffer_pool::BufferPool;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::*;
use protobuf::Message;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

// NOTE: Expects node to be non-full.
async fn insert_leaf<F: Filelike>(
    node: &mut Buffer<F, NodeProto>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    debug_assert!(node.get().has_leaf());
    let leaf: &mut LeafNodeProto = node.get_mut().mut_leaf();
    let idx = bp_tree::find_row_idx_for_key(leaf, key);
    leaf.keys.insert(idx, key);
    leaf.rows.insert(idx, row);
    Ok(())
}

// NOTE: Expects node to be non-full.
async fn insert_internal<F: Filelike>(
    table: &Table<F>,
    buffer_pool: &BufferPool<F>,
    mut node_buffer: MutexGuard<'_, Buffer<F, NodeProto>>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    let idx = bp_tree::find_next_node_idx_for_key(node_buffer.get().internal(), key)?;
    let mut child_mutex = buffer_pool
        .read_from_table(table, node_buffer.get().internal().child_offsets[idx])
        .await?;
    let mut child_buffer = child_mutex.lock().await;
    match &child_buffer.get().node_type {
        Some(node_proto::Node_type::Internal(_)) => {
            if child_buffer.would_overflow(std::mem::size_of::<i32>()) {
                let right_child_mutex = split_child_internal(
                    table,
                    buffer_pool,
                    &mut *node_buffer,
                    &mut child_buffer,
                    idx,
                )
                .await?;
                if node_buffer.get().internal().keys[idx] < key {
                    drop(child_buffer);
                    child_mutex = right_child_mutex;
                    child_buffer = child_mutex.lock().await;
                }
            }
            drop(node_buffer);
            return Box::pin(insert_internal(table, buffer_pool, child_buffer, key, row)).await;
        }
        Some(node_proto::Node_type::Leaf(_)) => {
            if child_buffer.would_overflow(row.compute_size() as usize) {
                let right_child_mutex = split_child_leaf(
                    table,
                    buffer_pool,
                    &mut *node_buffer,
                    &mut child_buffer,
                    idx,
                )
                .await?;
                if node_buffer.get().internal().keys[idx] < key {
                    drop(child_buffer);
                    child_mutex = right_child_mutex;
                    child_buffer = child_mutex.lock().await;
                }
            }
            drop(node_buffer);
            return insert_leaf(&mut *child_buffer, key, row).await;
        }
        None => unreachable!(),
    }
}

async fn split_child_leaf<F: Filelike>(
    table: &Table<F>,
    buffer_pool: &BufferPool<F>,
    parent: &mut Buffer<F, NodeProto>,
    child: &mut Buffer<F, NodeProto>,
    child_chunk_idx: usize,
) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
    log::trace!("Splitting leaf node.");
    debug_assert!(parent.get().has_internal());
    debug_assert!(child.get().has_leaf());
    let parent = parent.get_mut();
    let left_child = child.get_mut();

    let split_idx = left_child.leaf().keys.len() / 2;

    let right_child_mutex = buffer_pool.new_next_for_table(table).await?;
    let mut right_child_buffer = right_child_mutex.lock().await;
    let offset = right_child_buffer.offset;
    let right_child = right_child_buffer.get_mut();
    right_child.offset = offset;
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

    drop(right_child_buffer);
    Ok(right_child_mutex)
}

async fn split_child_internal<F: Filelike>(
    table: &Table<F>,
    buffer_pool: &BufferPool<F>,
    parent: &mut Buffer<F, NodeProto>,
    child: &mut Buffer<F, NodeProto>,
    child_chunk_idx: usize,
) -> Result<Arc<Mutex<Buffer<F, NodeProto>>>, Error> {
    log::trace!("Splitting internal node.");
    debug_assert!(parent.get().has_internal());
    debug_assert!(child.get().has_internal());
    let parent = parent.get_mut();
    let left_child = child.get_mut();

    let split_idx = left_child.internal().keys.len() / 2;

    let right_child_mutex = buffer_pool.new_next_for_table(table).await?;
    let mut right_child_buffer = right_child_mutex.lock().await;
    let offset = right_child_buffer.offset;
    let right_child = right_child_buffer.get_mut();
    right_child.offset = offset;
    right_child.parent_offset = parent.offset;
    right_child.mut_internal().keys = left_child.mut_internal().keys.split_off(split_idx);
    right_child.mut_internal().child_offsets =
        left_child.mut_internal().child_offsets.split_off(split_idx);

    let key = left_child.internal().keys[left_child.internal().keys.len() - 1];
    parent.mut_internal().keys.insert(child_chunk_idx, key);
    parent
        .mut_internal()
        .child_offsets
        .insert(child_chunk_idx + 1, right_child.offset);

    drop(right_child_buffer);
    Ok(right_child_mutex)
}

// NOTE: https://www.geeksforgeeks.org/insertion-in-a-b-tree/
// TODO: ensure key doesn't already exist
pub(crate) async fn insert<F: Filelike>(
    table: &Table<F>,
    buffer_pool: &BufferPool<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    let root_node_mutex = buffer_pool
        .read_from_table(table, table.root_chunk_offset)
        .await?;
    let mut root_buffer = root_node_mutex.lock().await;
    debug_assert!(root_buffer.get().has_internal());

    if root_buffer.get().internal().child_offsets.len() == 0 {
        log::trace!("Inserting first value.");

        let child_mutex = buffer_pool.new_next_for_table(table).await?;
        let mut child_buffer = child_mutex.lock().await;
        let offset = child_buffer.offset;
        let child_node = child_buffer.get_mut();
        child_node.offset = offset;
        child_node.parent_offset = root_buffer.get().offset;
        child_node.mut_leaf().keys.push(key);
        child_node.mut_leaf().rows.push(row);

        root_buffer
            .get_mut()
            .mut_internal()
            .child_offsets
            .push(child_node.offset);

        table.commit_metadata().await?;
        return Ok(());
    }

    if root_buffer.would_overflow(std::mem::size_of::<i32>()) {
        log::trace!("Root overflow detected.");

        let child_mutex = buffer_pool.new_next_for_table(table).await?;
        let mut child_buffer = child_mutex.lock().await;
        let offset = child_buffer.offset;
        let child_node = child_buffer.get_mut();
        child_node.offset = offset;
        child_node.set_internal(root_buffer.get().internal().clone());

        let root_internal = root_buffer.get_mut().mut_internal();
        root_internal.keys.clear();
        root_internal.child_offsets.clear();
        root_internal.child_offsets.push(child_node.offset);

        split_child_internal(table, buffer_pool, &mut *root_buffer, &mut *child_buffer, 0).await?;
    }

    insert_internal(table, buffer_pool, root_buffer, key, row).await?;
    table.commit_metadata().await?;

    Ok(())
}
