use crate::error::{Error, ErrorKind::*};
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::Table;
use crate::{
    DeleteStrategy::*, ReadStrategy::*, WriteStrategy::*, DELETE_STRATEGY, READ_STRATEGY,
    WRITE_STRATEGY,
};

mod insert_aggressive_split;
mod read_binary_search;
mod read_sequential;
mod unbalanced_delete;

// find what table of the current internal node's child nodes should be traversed
// next in order to find the row with the given key.
pub(crate) fn find_next_node_idx_for_key(
    internal: &InternalNodeProto,
    key: u32,
) -> Result<usize, Error> {
    match READ_STRATEGY {
        SequentialSearch => return read_sequential::find_next_node_idx_for_key(internal, key),
        BinarySearch => return read_binary_search::find_next_node_idx_for_key(internal, key),
    }
}

// find what table in the current leaf node the key should be placed.
// for read calls, this returns the row with the key, else the keys will mismatch.
// for write calls, this returns where the row should be inserted into the leaf.
pub(crate) fn find_row_idx_for_key(leaf: &LeafNodeProto, key: u32) -> usize {
    match READ_STRATEGY {
        SequentialSearch => read_sequential::find_row_idx_for_key(leaf, key),
        BinarySearch => read_binary_search::find_row_idx_for_key(leaf, key),
    }
}

// inserts the row with the associated key into the table.
pub(crate) async fn insert<F: Filelike>(
    table: &Table<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    match WRITE_STRATEGY {
        AggressiveSplit => insert_aggressive_split::insert::<F>(table, key, row).await,
    }
}

// Deletes the row with the given key, and returns it.
pub(crate) async fn delete<F: Filelike>(
    table: &Table<F>,
    key: u32,
) -> Result<InternalRowProto, Error> {
    match DELETE_STRATEGY {
        UnbalancedDelete => unbalanced_delete::delete(table, table.root_chunk_offset, key).await,
    }
}

// finds the row with the associated key, else returns NotFound.
pub(crate) async fn read_row<F: Filelike>(
    table: &Table<F>,
    curr_offset: u32,
    key: u32,
) -> Result<InternalRowProto, Error> {
    let node_buffer_lock = table
        .buffer_pool
        .read_from_table(table, curr_offset)
        .await?;
    let node_buffer = node_buffer_lock.read().await;
    match &node_buffer.get().node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            let idx = find_next_node_idx_for_key(&internal, key)?;
            let child_offset = internal.child_offsets[idx];
            drop(node_buffer);
            return Box::pin(read_row(table, child_offset, key)).await;
        }
        Some(node_proto::Node_type::Leaf(leaf)) => {
            let idx = find_row_idx_for_key(&leaf, key);
            if leaf.rows.len() <= idx || leaf.keys[idx] != key {
                return Err(Error::new(
                    NotFound,
                    format!("Row with key {} not found!", key),
                ));
            }
            return Ok(leaf.rows[idx].clone());
        }
        None => panic!(),
    }
}
