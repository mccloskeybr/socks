use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use std::io::{Read, Seek, Write};

pub(crate) mod insert_aggressive_split;
pub(crate) mod read_binary_search;
pub(crate) mod read_sequential;

pub fn find_next_node_for_key(
    config: &IndexConfig,
    internal: &InternalNodeProto,
    key: u32,
) -> Result<u32, Error> {
    match config.read_method.enum_value_or_default() {
        index_config::ReadMethod::INCREMENTAL => {
            return read_sequential::find_next_node_for_key(internal, key);
        }
        index_config::ReadMethod::BINARY_SEARCH => {
            return read_binary_search::find_next_node_for_key(internal, key);
        }
    }
}

pub fn find_row_for_key(
    config: &IndexConfig,
    leaf: &LeafNodeProto,
    key: u32,
) -> Result<InternalRowProto, Error> {
    match config.read_method.enum_value_or_default() {
        index_config::ReadMethod::INCREMENTAL => {
            return read_sequential::find_row_for_key(leaf, key);
        }
        index_config::ReadMethod::BINARY_SEARCH => {
            return read_binary_search::find_row_for_key(leaf, key);
        }
    }
}

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
            let node_id =
                find_next_node_for_key(index.metadata.config.as_ref().unwrap(), &internal, key)?;
            return read_row(index, node_id, key);
        }
        Some(node_proto::Node_type::Leaf(leaf)) => {
            return find_row_for_key(index.metadata.config.as_ref().unwrap(), &leaf, key);
        }
        None => panic!(),
    }
    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}

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
