use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use crate::{BINARY_READ_ITER_CUTOFF, LANE_WIDTH};
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom, Write};
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::{LaneCount, Mask, Simd, SupportedLaneCount};

// Creates a simd vector of indices evenly distributed in the range of low, high.
// e.g. 0, 100 w/ lane width of 4 --> [20, 40, 60, 80].
#[inline]
fn fan_over_range(low: usize, high: usize) -> Simd<usize, LANE_WIDTH> {
    let step = (high - low + 1) / (LANE_WIDTH + 1);
    let mut idxs = Vec::with_capacity(LANE_WIDTH);
    for i in 0..LANE_WIDTH {
        idxs.push(low + i * step);
    }
    debug_assert_eq!(idxs.len(), LANE_WIDTH);
    Simd::from_slice(&idxs)
}

// retrieves which child node to traverse to find the provided key.
pub fn find_next_node_for_key(internal: &InternalNodeProto, key: u32) -> Result<u32, Error> {
    if internal.keys.len() == 0 {
        debug_assert!(internal.child_ids.len() > 0);
        return Ok(internal.child_ids[0]);
    }

    let keys = Simd::<u32, LANE_WIDTH>::splat(key);

    let mut lower: usize = 0;
    let mut upper: usize = std::cmp::max(internal.keys.len(), 1) - 1;
    while upper - lower > BINARY_READ_ITER_CUTOFF {
        let idxs: Simd<usize, LANE_WIDTH> = fan_over_range(lower, upper);
        let test_keys = Simd::gather_or_default(&internal.keys, idxs);
        let comp: Mask<isize, LANE_WIDTH> = keys.simd_lt(test_keys).into();
        match comp.first_set() {
            None => {
                lower = idxs.to_array()[LANE_WIDTH - 1];
            }
            Some(i) if i == 0 => {
                upper = idxs.to_array()[0];
            }
            Some(i) => {
                upper = idxs.to_array()[i];
                lower = idxs.to_array()[std::cmp::max(i, 1) - 1];
            }
            _ => unreachable!(),
        }
        debug_assert!(lower <= upper);
    }

    let mut idx: usize = lower;
    for chunk in internal
        .keys
        .get(lower..upper + 1)
        .unwrap()
        .chunks(LANE_WIDTH)
    {
        let test_keys = Simd::<u32, LANE_WIDTH>::load_or_default(chunk);
        let mask = keys.simd_lt(test_keys);
        match mask.first_set() {
            Some(j) => {
                idx += j;
                return Ok(internal.child_ids[idx + (key == internal.keys[idx]) as usize]);
            }
            None => {}
        }
        idx += chunk.len();
    }

    if internal.keys.len() != internal.child_ids.len() {
        debug_assert!(internal.child_ids.len() == internal.keys.len() + 1);
        return Ok(internal.child_ids[internal.child_ids.len() - 1]);
    }

    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}

pub fn find_row_for_key(leaf: &LeafNodeProto, key: u32) -> Result<InternalRowProto, Error> {
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

    Err(Error::NotFound(format!("Row with key {} not found!", key)))
}
