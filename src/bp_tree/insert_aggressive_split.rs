use std::io::{Read, Write, Seek};
use std::simd::{Simd, LaneCount, SupportedLaneCount};
use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use protobuf::Message;
use protobuf::MessageField;
use crate::index::*;
use crate::error::*;
use crate::parse::*;
use crate::file::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::config::*;
use crate::protos::generated::chunk::*;

fn insert_non_full_leaf<const N: usize, F: Read + Write + Seek>(
    index: &mut Index<F>, node_chunk: &mut ChunkProto, key: u32, row: InternalRowProto)
-> Result<(), Error>
where LaneCount<N>: SupportedLaneCount {
    debug_assert!(node_chunk.has_node());
    debug_assert!(node_chunk.node().has_leaf());

    let leaf: &mut LeafNodeProto = node_chunk.mut_node().mut_leaf();
    let mut idx = 0;
    let keys = Simd::<u32, N>::splat(key);
    for chunk in leaf.keys.chunks(N) {
        let test_keys = Simd::<u32, N>::load_or_default(chunk);
        let mask = keys.simd_lt(test_keys);
        match mask.first_set() {
            Some(j) => { idx += j; break; },
            None => {}
        }
        idx += chunk.len();
    }

    leaf.keys.insert(idx, key);
    leaf.rows.insert(idx, row);
    row_data::commit_chunk(index, &node_chunk)?;
    Ok(())
}

fn insert_non_full_internal<const N: usize, F: Read + Write + Seek>(
    index: &mut Index<F>, node_chunk: &mut ChunkProto, key: u32, row: InternalRowProto)
-> Result<(), Error>
where LaneCount<N>: SupportedLaneCount {
    debug_assert!(node_chunk.has_node());
    debug_assert!(node_chunk.node().has_internal());

    let mut idx = 0;
    let keys = Simd::<u32, N>::splat(key);
    for chunk in node_chunk.node().internal().keys.chunks(N) {
        let test_keys = Simd::<u32, N>::load_or_default(chunk);
        let mask = keys.simd_lt(test_keys);
        match mask.first_set() {
            Some(j) => { idx += j; break; },
            None => {}
        }
        idx += chunk.len();
    }

    debug_assert!(idx < node_chunk.node().internal().child_ids.len());
    let mut child_chunk = row_data::find_chunk(index, node_chunk.node().internal().child_ids[idx])?;
    debug_assert!(child_chunk.has_node());
    match &child_chunk.mut_node().node_type {
        Some(node_proto::Node_type::Internal(_)) => {
            if chunk::would_chunk_overflow(&index.db_config.file,
                                           child_chunk.compute_size() as usize +
                                           std::mem::size_of::<i32>()) {
                let right_child = split_child_internal(index, node_chunk, &mut child_chunk, idx)?;
                if node_chunk.node().internal().keys[idx] < key {
                    child_chunk = right_child;
                }
            }
            return insert_non_full_internal::<N, F>(index, &mut child_chunk, key, row);
        },
        Some(node_proto::Node_type::Leaf(_)) => {
            if chunk::would_chunk_overflow(&index.db_config.file,
                                           child_chunk.compute_size() as usize + row.compute_size() as usize) {
                let right_child = split_child_leaf(index, node_chunk, &mut child_chunk, idx)?;
                if node_chunk.node().internal().keys[idx] < key {
                    child_chunk = right_child;
                }
            }
            return insert_non_full_leaf::<N, F>(index, &mut child_chunk, key, row);
        },
        None => unreachable!(),
    }
}

fn split_child_leaf<F: Read + Write + Seek>(
    index: &mut Index<F>, parent_chunk: &mut ChunkProto, left_child_chunk: &mut ChunkProto,
    child_chunk_idx: usize)
-> Result<ChunkProto, Error> {
    log::trace!("Splitting leaf node.");
    debug_assert!(parent_chunk.node().has_internal());
    debug_assert!(left_child_chunk.node().has_leaf());
    let parent: &mut InternalNodeProto = parent_chunk.mut_node().mut_internal();
    let left_child: &mut LeafNodeProto = left_child_chunk.mut_node().mut_leaf();

    let mut split_idx = left_child.keys.len() / 2;

    let mut right_child_chunk = ChunkProto::new();
    let mut right_child = right_child_chunk.mut_node();
    right_child.id = metadata::next_chunk_id(index);
    right_child.mut_leaf().keys = left_child.keys.split_off(split_idx);
    right_child.mut_leaf().rows = left_child.rows.split_off(split_idx);

    parent.keys.insert(child_chunk_idx, right_child.leaf().keys[0]);
    parent.child_ids.insert(child_chunk_idx + 1, right_child.id);

    row_data::commit_chunk(index, left_child_chunk)?;
    row_data::commit_chunk(index, &right_child_chunk)?;
    row_data::commit_chunk(index, parent_chunk)?;

    Ok(right_child_chunk)
}

fn split_child_internal<F: Read + Write + Seek>(
    index: &mut Index<F>, parent_chunk: &mut ChunkProto, left_child_chunk: &mut ChunkProto,
    child_chunk_idx: usize)
-> Result<ChunkProto, Error> {
    log::trace!("Splitting internal node.");
    debug_assert!(parent_chunk.node().has_internal());
    debug_assert!(left_child_chunk.node().has_internal());
    let parent: &mut InternalNodeProto = parent_chunk.mut_node().mut_internal();
    let left_child: &mut InternalNodeProto = left_child_chunk.mut_node().mut_internal();

    let mut split_idx = left_child.keys.len() / 2;

    let mut right_child_chunk = ChunkProto::new();
    let mut right_child = right_child_chunk.mut_node();
    right_child.id = metadata::next_chunk_id(index);
    right_child.mut_internal().keys = left_child.keys.split_off(split_idx);
    right_child.mut_internal().child_ids = left_child.child_ids.split_off(split_idx);

    parent.keys.insert(child_chunk_idx, left_child.keys[left_child.keys.len() - 1]);
    parent.child_ids.insert(child_chunk_idx + 1, right_child.id);

    row_data::commit_chunk(index, left_child_chunk)?;
    row_data::commit_chunk(index, &right_child_chunk)?;
    row_data::commit_chunk(index, parent_chunk)?;

    Ok(right_child_chunk)
}

// NOTE: https://www.geeksforgeeks.org/insertion-in-a-b-tree/
// TODO: ensure key doesn't already exist
pub fn insert<const N: usize, F: Read + Write + Seek>(
    index: &mut Index<F>, key: u32, row: InternalRowProto)
-> Result<(), Error>
where LaneCount<N>: SupportedLaneCount {
    let mut root_chunk = row_data::find_chunk(index, index.metadata.root_chunk_id)?;
    debug_assert!(root_chunk.node().has_internal());

    if root_chunk.node().internal().keys.len() +
        root_chunk.node().internal().child_ids.len() == 0 {
        log::trace!("Inserting first value.");

        let mut data_chunk = ChunkProto::new();
        let mut data = data_chunk.mut_node();
        data.id = metadata::next_chunk_id(index);
        data.mut_leaf().keys.push(key);
        data.mut_leaf().rows.push(row);

        root_chunk.mut_node().mut_internal().child_ids.push(data.id);

        row_data::commit_chunk(index, &data_chunk)?;
        row_data::commit_chunk(index, &root_chunk)?;

        metadata::commit_metadata(index)?;
        return Ok(());
    }

    if chunk::would_chunk_overflow(&index.db_config.file,
                                root_chunk.compute_size() as usize +
                                std::mem::size_of::<i32>()) {
        log::trace!("Root overflow detected.");

        // TODO: this is inefficient.
        let mut child_chunk = root_chunk.clone();
        child_chunk.mut_node().id = metadata::next_chunk_id(index);

        root_chunk.mut_node().mut_internal().keys.clear();
        root_chunk.mut_node().mut_internal().child_ids.clear();
        root_chunk.mut_node().mut_internal().child_ids.push(child_chunk.node().id);

        split_child_internal(index, &mut root_chunk, &mut child_chunk, 0)?;
    }
    insert_non_full_internal::<N, F>(index, &mut root_chunk, key, row)?;
    metadata::commit_metadata(index)?;
    Ok(())
}
