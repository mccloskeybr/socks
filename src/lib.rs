#![feature(portable_simd)]

// How many SIMD lanes to use.
// Used to accelerate key comparison during B+ tree traversal.
static LANE_WIDTH: usize = 8;

// The size of each file chunk, in bytes. Influences various parts of the
// database, e.g. the size of each B+ tree node, or how many query results are
// grouped together. Each chunk stores 1 protobuf, so the value must be less
// than the maximum protobuf size (2GiB).
static BUFFER_SIZE: usize = 4096;

// The byte size buffer before considering a chunk as full.
// TODO: this shouldn't be required if calculating proto sizes correctly.
static BUFFER_OVERFLOW_BUFFER: usize = 5;

// A basic LRU cache is used to speed up read / write operations to frequently
// accessed chunks. It is sharded to lower thread contention.
static BUFFER_POOL_SHARD_COUNT: usize = 16;

// The size (in chunks) of each cache shard before evicting the least
// frequently used chunk. This effectively means socks can store a maximum of
// BUFFER_POOL_SHARD_COUNT * BUFFER_POOL_SHARD_SIZE chunks in memory at any given time.
static BUFFER_POOL_SHARD_SIZE: usize = 16;

// When searching through table B+ tree nodes using a binary search, this is the
// number of remaining elements left until the algorithm switches to a sequential
// search. This is better for cache coherence when sufficiently low.
static BINARY_READ_ITER_CUTOFF: usize = 100;

// Configurable read strategies for table B+ tree traversal.
#[allow(dead_code)]
enum ReadStrategy {
    SequentialSearch,
    BinarySearch,
}
static READ_STRATEGY: ReadStrategy = ReadStrategy::BinarySearch;

// Configurable write strategies for B+ tree insertion.
#[allow(dead_code)]
enum WriteStrategy {
    AggressiveSplit,
}
static WRITE_STRATEGY: WriteStrategy = WriteStrategy::AggressiveSplit;

extern crate self as socks;
mod bp_tree;
mod buffer;
mod buffer_pool;
pub mod database;
mod error;
mod filelike;
mod protos;
mod query;
mod schema;
mod table;
