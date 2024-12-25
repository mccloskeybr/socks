#![allow(warnings)]
#![feature(portable_simd)]

// How many SIMD lanes to use.
// Used to accelerate key comparison during B+ tree traversal.
static LANE_WIDTH: usize = 8;

// Number of chunks to include in the LRU cache. Helps speed up read operations by
// saving on disk i/o for chunks already present in the cache.
static CACHE_SIZE: usize = 10;

// When searching through table B+ tree nodes using a binary search, this is the
// number of remaining elements left until the algorithm switches to a sequential
// search. This is better for cache coherence when sufficiently low.
static BINARY_READ_ITER_CUTOFF: usize = 10;

// The size of each file chunk, in bytes. Influences various parts of the
// database, e.g. the size of each B+ tree node, or how many query results are
// grouped together. Each chunk stores 1 protobuf, so the value must be less
// than the maximum protobuf size (2GiB).
static CHUNK_SIZE: usize = 4096;

// The byte size buffer before considering a chunk as full.
// TODO: this shouldn't be required if calculating proto sizes correctly.
static CHUNK_OVERFLOW_BUFFER: usize = 5;

// Configurable read strategies for table B+ tree traversal.
enum ReadStrategy {
    SequentialSearch,
    BinarySearch,
}
static READ_STRATEGY: ReadStrategy = ReadStrategy::BinarySearch;

// Configurable write strategies for B+ tree insertion.
enum WriteStrategy {
    AggressiveSplit,
}
static WRITE_STRATEGY: WriteStrategy = WriteStrategy::AggressiveSplit;

extern crate self as socks;
mod bp_tree;
mod cache;
mod chunk;
pub mod database;
mod error;
mod filelike;
mod protos;
mod query;
mod schema;
mod table;
mod validate;
