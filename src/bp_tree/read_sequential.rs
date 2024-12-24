use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::table::*;
use crate::LANE_WIDTH;
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom, Write};
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::{LaneCount, Simd, SupportedLaneCount};

pub fn find_next_node_idx_for_key(internal: &InternalNodeProto, key: u32) -> Result<usize, Error> {
    let mut idx = 0;
    let keys = Simd::<u32, LANE_WIDTH>::splat(key);
    for chunk in internal.keys.chunks(LANE_WIDTH) {
        let test_keys = Simd::<u32, LANE_WIDTH>::load_or_default(chunk);
        let mask = keys.simd_le(test_keys);
        match mask.first_set() {
            Some(j) => {
                idx += j;
                let child_idx = idx + (key == internal.keys[idx]) as usize;
                return Ok(child_idx);
            }
            None => {}
        }
        idx += chunk.len();
    }

    if internal.keys.len() != internal.child_offsets.len() {
        debug_assert!(internal.child_offsets.len() == internal.keys.len() + 1);
        return Ok(internal.child_offsets.len() - 1);
    }

    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}

pub fn find_row_idx_for_key(leaf: &LeafNodeProto, key: u32) -> usize {
    let mut idx = 0;
    let keys = Simd::<u32, LANE_WIDTH>::splat(key);
    for chunk in leaf.keys.chunks(LANE_WIDTH) {
        let test_keys = Simd::<u32, LANE_WIDTH>::load_or_default(chunk);
        let mask = keys.simd_le(test_keys);
        match mask.first_set() {
            Some(j) => {
                idx += j;
                return idx;
            }
            None => {}
        }
        idx += chunk.len();
    }
    idx
}
