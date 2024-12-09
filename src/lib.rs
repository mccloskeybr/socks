#![allow(warnings)]
#![feature(portable_simd)]

// How many SIMD lanes to use.
// Used to accelerate key comparison during B+ tree traversal.
static LANE_WIDTH: usize = 8;

static BINARY_READ_ITER_CUTOFF: usize = 10;

extern crate self as socks;

mod bp_tree;
mod error;
mod file;
pub mod index;
mod parse;
mod protos;
mod stats;
