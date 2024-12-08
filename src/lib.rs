#![allow(warnings)]
#![feature(portable_simd)]

static LANE_WIDTH: usize = 16;

extern crate self as socks;

mod bp_tree;
mod error;
mod file;
pub mod index;
mod parse;
mod protos;
mod stats;
