#[cfg(test)]
#[path = "./table_test.rs"]
mod test;

use crate::bp_tree;
use crate::cache::Cache;
use crate::chunk;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use protobuf::Message;
use protobuf::MessageField;
use std::io::{Read, Seek, Write};

// Table file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub(crate) struct Table<F: Filelike> {
    pub(crate) file: F,
    pub(crate) cache: Cache,
    pub(crate) metadata: TableMetadataProto,
}

pub(crate) fn create<F: Filelike>(
    mut file: F,
    name: String,
    schema: TableSchema,
) -> Result<Table<F>, Error> {
    let mut metadata = TableMetadataProto::new();
    metadata.name = name;
    metadata.schema = MessageField::some(schema);
    metadata.root_chunk_offset = 1;
    metadata.next_chunk_offset = 2;
    {
        chunk::write_chunk_at::<F, TableMetadataProto>(&mut file, metadata.clone(), 0)?;
    }
    {
        let mut root_node = NodeProto::new();
        root_node.offset = 1;
        root_node.set_internal(InternalNodeProto::new());
        chunk::write_chunk_at::<F, NodeProto>(&mut file, root_node, 1)?;
    }

    Ok(Table {
        file: file,
        cache: Cache::default(),
        metadata: metadata,
    })
}

pub(crate) fn next_chunk_offset<F: Filelike>(table: &mut Table<F>) -> u32 {
    let offset = table.metadata.next_chunk_offset;
    table.metadata.next_chunk_offset += 1;
    offset
}

pub(crate) fn commit_metadata<F: Filelike>(table: &mut Table<F>) -> Result<(), Error> {
    log::trace!("Committing metadata.");
    chunk::write_chunk_at::<F, TableMetadataProto>(&mut table.file, table.metadata.clone(), 0)?;
    Ok(())
}

pub(crate) fn is_table_keyed_on_column<F: Filelike>(table: &Table<F>, col_name: &str) -> bool {
    schema::is_schema_keyed_on_column(&table.metadata.schema, col_name)
}

pub(crate) fn insert<F: Filelike>(
    table: &mut Table<F>,
    key: u32,
    row: InternalRowProto,
) -> Result<(), Error> {
    log::trace!("Inserting row: {row}");
    bp_tree::insert(table, key, row)
}

pub(crate) fn read_row<F: Filelike>(table: &mut Table<F>, key: u32) -> Result<RowProto, Error> {
    log::trace!("Retrieving row with key: {key}");
    let internal_row = bp_tree::read_row(table, table.metadata.root_chunk_offset, key)?;
    Ok(schema::internal_row_to_row(
        &internal_row,
        &table.metadata.schema,
    ))
}
