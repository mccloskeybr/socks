use crate::bp_tree;
use crate::error::{Error, ErrorKind::*};
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::table::Table;

pub(crate) async fn delete<F: Filelike>(
    table: &Table<F>,
    curr_offset: u32,
    key: u32,
) -> Result<InternalRowProto, Error> {
    let node_buffer_lock = table
        .buffer_pool
        .read_from_table(table, curr_offset)
        .await?;
    let node_buffer = node_buffer_lock.read().await;
    match &node_buffer.get().node_type {
        Some(node_proto::Node_type::Internal(internal)) => {
            let idx = bp_tree::find_next_node_idx_for_key(&internal, key)?;
            let child_offset = internal.child_offsets[idx];
            drop(node_buffer);
            return Box::pin(delete(table, child_offset, key)).await;
        }
        Some(node_proto::Node_type::Leaf(_)) => {
            drop(node_buffer);
            let mut node_buffer = node_buffer_lock.write().await;
            let leaf = node_buffer.get_mut().mut_leaf();
            let idx = bp_tree::find_row_idx_for_key(&leaf, key);
            if leaf.rows.len() <= idx || leaf.keys[idx] != key {
                return Err(Error::new(
                    NotFound,
                    format!("Row with key {} not found!", key),
                ));
            }
            leaf.keys.remove(idx);
            let row = leaf.rows.remove(idx);
            return Ok(row);
        }
        None => panic!(),
    }
}
