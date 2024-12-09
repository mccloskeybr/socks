use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use std::io::{Read, Seek, Write};

pub(crate) mod insert_aggressive_split;
pub(crate) mod read_binary_search;
pub(crate) mod read_sequential;

// find what index of the current internal node's child nodes should be traversed
// next in order to find the row with the given key.
pub fn find_next_node_idx_for_key(
    config: &IndexConfig,
    internal: &InternalNodeProto,
    key: u32,
) -> Result<usize, Error> {
    match config.read_method.enum_value_or_default() {
        index_config::ReadMethod::INCREMENTAL => {
            return read_sequential::find_next_node_idx_for_key(internal, key);
        }
        index_config::ReadMethod::BINARY_SEARCH => {
            return read_binary_search::find_next_node_idx_for_key(internal, key);
        }
    }
}

// find what index in the current leaf node the key should be placed.
// for read calls, this returns the row with the key, else the keys will mismatch.
// for write calls, this returns where the row should be inserted into node.
pub fn find_row_idx_for_key(config: &IndexConfig, leaf: &LeafNodeProto, key: u32) -> usize {
    match config.read_method.enum_value_or_default() {
        index_config::ReadMethod::INCREMENTAL => {
            return read_sequential::find_row_idx_for_key(leaf, key);
        }
        index_config::ReadMethod::BINARY_SEARCH => {
            return read_binary_search::find_row_idx_for_key(leaf, key);
        }
    }
}

// finds the row with the associated key, else returns NotFound.
pub fn read_row<F: Read + Write + Seek>(
    index: &mut Index<F>,
    curr_id: u32,
    key: u32,
) -> Result<InternalRowProto, Error> {
    let curr_chunk: ChunkProto = row_data::find_chunk(index, curr_id)?;
    debug_assert!(curr_chunk.has_node());
    let node: &NodeProto = curr_chunk.node();

    match &node.node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            let idx = find_next_node_idx_for_key(
                index.metadata.config.as_ref().unwrap(),
                &internal,
                key,
            )?;
            return read_row(index, internal.child_ids[idx], key);
        }
        Some(node_proto::Node_type::Leaf(leaf)) => {
            let idx = find_row_idx_for_key(index.metadata.config.as_ref().unwrap(), &leaf, key);
            if leaf.rows.len() <= idx || leaf.keys[idx] != key {
                return Err(Error::NotFound(format!("Row with key {} not found!", key)));
            }
            return Ok(leaf.rows[idx].clone());
        }
        None => panic!(),
    }
    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}

// inserts the row with the associated key into the index.
pub fn insert<F: Read + Write + Seek>(
    index: &mut Index<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    match index.metadata.config.insert_method.enum_value_or_default() {
        index_config::InsertMethod::AGGRESSIVE_SPLIT => {
            return insert_aggressive_split::insert::<F>(index, key, row);
        }
    }
}
