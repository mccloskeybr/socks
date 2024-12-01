use std::io::{Read, Write, Seek, SeekFrom};
use crate::index::*;
use crate::file::*;
use crate::error::*;
use crate::protos::generated::chunk::*;

pub fn read_row<F: Read + Write + Seek>(index: &mut Index<F>, curr_id: u32, key: String)
-> Result<InternalRowProto, Error> {
    let curr_chunk: ChunkProto = row_data::find_chunk(index, curr_id)?;
    debug_assert!(curr_chunk.has_data());
    let data: &DataProto = curr_chunk.data();

    for i in 0..data.values.len() {
        match &data.values[i].node_type {
            Some(data_proto::value::Node_type::ChildId(child_id)) => {
                if i == data.values.len() - 1 {
                    return read_row(index, *child_id, key);
                }
            }
            Some(data_proto::value::Node_type::RowNode(row_node)) => {
                if key < row_node.key {
                    debug_assert!(i > 0);
                    let child_ref: &data_proto::Value = &data.values[i - 1];
                    debug_assert!(child_ref.has_child_id());
                    return read_row(index, child_ref.child_id(), key);
                }
                else if key == row_node.key {
                    log::trace!("Found row {} in node {}.", key, curr_id);
                    return Ok(row_node.clone());
                }
            },
            None => {
                return Err(Error::Internal(format!("Unexpectedly found an empty node: {}!", curr_id).into()));
            },
        }
    }
    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}
