use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::table::cache;
use crate::table::table::*;

mod insert_aggressive_split;
mod read_binary_search;
mod read_sequential;

// find what table of the current internal node's child nodes should be traversed
// next in order to find the row with the given key.
pub fn find_next_node_idx_for_key(
    config: &TableConfig,
    internal: &InternalNodeProto,
    key: u32,
) -> Result<usize, Error> {
    match config.read_method.enum_value_or_default() {
        table_config::ReadMethod::INCREMENTAL => {
            return read_sequential::find_next_node_idx_for_key(internal, key);
        }
        table_config::ReadMethod::BINARY_SEARCH => {
            return read_binary_search::find_next_node_idx_for_key(internal, key);
        }
    }
}

// find what table in the current leaf node the key should be placed.
// for read calls, this returns the row with the key, else the keys will mismatch.
// for write calls, this returns where the row should be inserted into the leaf.
pub fn find_row_idx_for_key(config: &TableConfig, leaf: &LeafNodeProto, key: u32) -> usize {
    match config.read_method.enum_value_or_default() {
        table_config::ReadMethod::INCREMENTAL => {
            return read_sequential::find_row_idx_for_key(leaf, key);
        }
        table_config::ReadMethod::BINARY_SEARCH => {
            return read_binary_search::find_row_idx_for_key(leaf, key);
        }
    }
}

// finds the row with the associated key, else returns NotFound.
pub fn read_row<F: Filelike>(
    table: &mut Table<F>,
    curr_offset: u32,
    key: u32,
) -> Result<InternalRowProto, Error> {
    let curr_chunk: ChunkProto = cache::read(table, curr_offset)?;
    debug_assert!(curr_chunk.has_node());
    let node: &NodeProto = curr_chunk.node();

    match &node.node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            let idx = find_next_node_idx_for_key(
                table.metadata.config.as_ref().unwrap(),
                &internal,
                key,
            )?;
            return read_row(table, internal.child_offsets[idx], key);
        }
        Some(node_proto::Node_type::Leaf(leaf)) => {
            let idx = find_row_idx_for_key(table.metadata.config.as_ref().unwrap(), &leaf, key);
            if leaf.rows.len() <= idx || leaf.keys[idx] != key {
                return Err(Error::NotFound(format!("Row with key {} not found!", key)));
            }
            return Ok(leaf.rows[idx].clone());
        }
        None => panic!(),
    }
    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}

// inserts the row with the associated key into the table.
pub fn insert<F: Filelike>(
    table: &mut Table<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    match table.metadata.config.insert_method.enum_value_or_default() {
        table_config::InsertMethod::AGGRESSIVE_SPLIT => {
            return insert_aggressive_split::insert::<F>(table, key, row);
        }
    }
}
