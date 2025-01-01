use crate::buffer_pool::BufferPool;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::*;
use crate::{ReadStrategy::*, WriteStrategy::*, READ_STRATEGY, WRITE_STRATEGY};

mod insert_aggressive_split;
mod read_binary_search;
mod read_sequential;

// find what table of the current internal node's child nodes should be traversed
// next in order to find the row with the given key.
pub(crate) fn find_next_node_idx_for_key(
    internal: &InternalNodeProto,
    key: u32,
) -> Result<usize, Error> {
    match READ_STRATEGY {
        SequentialSearch => {
            return read_sequential::find_next_node_idx_for_key(internal, key);
        }
        BinarySearch => {
            return read_binary_search::find_next_node_idx_for_key(internal, key);
        }
    }
}

// find what table in the current leaf node the key should be placed.
// for read calls, this returns the row with the key, else the keys will mismatch.
// for write calls, this returns where the row should be inserted into the leaf.
pub(crate) fn find_row_idx_for_key(leaf: &LeafNodeProto, key: u32) -> usize {
    match READ_STRATEGY {
        SequentialSearch => {
            return read_sequential::find_row_idx_for_key(leaf, key);
        }
        BinarySearch => {
            return read_binary_search::find_row_idx_for_key(leaf, key);
        }
    }
}

// finds the row with the associated key, else returns NotFound.
pub(crate) async fn read_row<F: Filelike>(
    table: &mut Table<F>,
    buffer_pool: &mut BufferPool<F>,
    curr_offset: u32,
    key: u32,
) -> Result<InternalRowProto, Error> {
    let node_buffer_mutex = buffer_pool.read_from_table(table, curr_offset).await?;
    let node_buffer = node_buffer_mutex.lock().await;
    match &node_buffer.get().node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            let idx = find_next_node_idx_for_key(&internal, key)?;
            let child_offset = internal.child_offsets[idx];
            drop(node_buffer);
            return Box::pin(read_row(table, buffer_pool, child_offset, key)).await;
        }
        Some(node_proto::Node_type::Leaf(leaf)) => {
            let idx = find_row_idx_for_key(&leaf, key);
            if leaf.rows.len() <= idx || leaf.keys[idx] != key {
                return Err(Error::NotFound(format!("Row with key {} not found!", key)));
            }
            return Ok(leaf.rows[idx].clone());
        }
        None => panic!(),
    }
}

// inserts the row with the associated key into the table.
pub(crate) async fn insert<F: Filelike>(
    table: &mut Table<F>,
    buffer_pool: &mut BufferPool<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    match WRITE_STRATEGY {
        AggressiveSplit => {
            return insert_aggressive_split::insert::<F>(table, buffer_pool, key, row).await;
        }
    }
}
