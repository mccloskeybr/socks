#![allow(warnings)]
#![feature(portable_simd)]

// How many SIMD lanes to use.
// Used to accelerate key comparison during B+ tree traversal.
static LANE_WIDTH: usize = 8;

static BINARY_READ_ITER_CUTOFF: usize = 10;

extern crate self as socks;
mod error;
mod protos;
mod stats;
pub mod table;
