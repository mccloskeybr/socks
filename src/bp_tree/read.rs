use std::io::{Read, Write, Seek, SeekFrom};
use std::cmp::Ordering;
use std::simd::{Simd, LaneCount, SupportedLaneCount};
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use crate::index::*;
use crate::file::*;
use crate::error::*;
use crate::protos::generated::chunk::*;

// TODO: binary search.

pub fn read_row<const N: usize, F: Read + Write + Seek>(
    index: &mut Index<F>, curr_id: u32, key: u32)
-> Result<InternalRowProto, Error>
where LaneCount<N>: SupportedLaneCount {
    let curr_chunk: ChunkProto = row_data::find_chunk(index, curr_id)?;
    debug_assert!(curr_chunk.has_node());
    let node: &NodeProto = curr_chunk.node();

    match &node.node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            let mut idx = 0;
            let keys = Simd::<u32, N>::splat(key);
            for chunk in internal.keys.chunks(N) {
                let test_keys = Simd::<u32, N>::load_or_default(chunk);
                let mask = keys.simd_le(test_keys);
                match mask.first_set() {
                    Some(j) => {
                        idx += j;
                        let child_idx = idx + (key == internal.keys[idx]) as usize;
                        return read_row(index, internal.child_ids[child_idx], key);
                    },
                    None => {}
                }
                idx += chunk.len();
            }
            if internal.keys.len() != internal.child_ids.len() {
                debug_assert!(internal.child_ids.len() == internal.keys.len() + 1);
                return read_row(index, internal.child_ids[internal.child_ids.len() - 1], key);
            }
        },
        Some(node_proto::Node_type::Leaf(leaf)) => {
            let mut idx = 0;
            let keys = Simd::<u32, N>::splat(key);
            for chunk in leaf.keys.chunks(N) {
                let test_keys = Simd::<u32, N>::load_or_default(chunk);
                let mask = keys.simd_eq(test_keys);
                match mask.first_set() {
                    Some(j) => {
                        idx += j;
                        return Ok(leaf.rows[idx].clone());
                    },
                    None => {}
                }
                idx += chunk.len();
            }
        },
        None => panic!(),
    }
    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}
