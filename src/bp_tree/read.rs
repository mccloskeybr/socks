use std::io::{Read, Write, Seek, SeekFrom};
use crate::index::*;
use crate::file::*;
use crate::error::*;
use crate::protos::generated::chunk::*;

pub fn read_row<F: Read + Write + Seek>(
    index: &mut Index<F>, curr_id: u32, key: String)
-> Result<InternalRowProto, Error> {
    let curr_chunk: ChunkProto = row_data::find_chunk(index, curr_id)?;
    debug_assert!(curr_chunk.has_node());
    let node: &NodeProto = curr_chunk.node();

    match &node.node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            for i in 0..internal.values.len() {
                let value: &internal_node_proto::Value = &internal.values[i];
                match &value.value_type {
                    Some(internal_node_proto::value::Value_type::Key(test_key)) => {
                        let test_key = test_key.to_string();
                        if key <= test_key {
                            let idx = i - (key < test_key) as usize + (key == test_key) as usize;
                            debug_assert!(0 <= idx && idx < internal.values.len());
                            let child_ref: &internal_node_proto::Value = &internal.values[idx];
                            debug_assert!(child_ref.has_child_id());
                            return read_row(index, child_ref.child_id(), key);
                        }
                    },
                    Some(internal_node_proto::value::Value_type::ChildId(child_id)) => {
                        if i == internal.values.len() - 1 {
                            return read_row(index, *child_id, key);
                        }
                    }
                    _ => {},
                }
            }
        },
        Some(node_proto::Node_type::Leaf(leaf)) => {
            for row in &leaf.rows {
                if row.key == key.to_string() { return Ok(row.clone()); }
            }
        },
        None => unreachable!(),
    }
    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}
