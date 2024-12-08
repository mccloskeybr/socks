use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use crate::LANE_WIDTH;
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom, Write};
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::{LaneCount, Simd, SupportedLaneCount};

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
            let mut idx = 0;
            let keys = Simd::<u32, LANE_WIDTH>::splat(key);
            for chunk in internal.keys.chunks(LANE_WIDTH) {
                let test_keys = Simd::<u32, LANE_WIDTH>::load_or_default(chunk);
                let mask = keys.simd_le(test_keys);
                match mask.first_set() {
                    Some(j) => {
                        idx += j;
                        let child_idx = idx + (key == internal.keys[idx]) as usize;
                        return read_row(index, internal.child_ids[child_idx], key);
                    }
                    None => {}
                }
                idx += chunk.len();
            }
            if internal.keys.len() != internal.child_ids.len() {
                debug_assert!(internal.child_ids.len() == internal.keys.len() + 1);
                return read_row(index, internal.child_ids[internal.child_ids.len() - 1], key);
            }
        }
        Some(node_proto::Node_type::Leaf(leaf)) => {
            let mut idx = 0;
            let keys = Simd::<u32, LANE_WIDTH>::splat(key);
            for chunk in leaf.keys.chunks(LANE_WIDTH) {
                let test_keys = Simd::<u32, LANE_WIDTH>::load_or_default(chunk);
                let mask = keys.simd_eq(test_keys);
                match mask.first_set() {
                    Some(j) => {
                        idx += j;
                        return Ok(leaf.rows[idx].clone());
                    }
                    None => {}
                }
                idx += chunk.len();
            }
        }
        None => panic!(),
    }
    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}
