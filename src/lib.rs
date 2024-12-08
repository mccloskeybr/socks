#![allow(warnings)]
#![feature(portable_simd)]

extern crate self as socks;

mod bp_tree;
mod error;
mod file;
pub mod index;
mod parse;
mod protos;
mod stats;
