#[cfg(test)]
#[path = "./table_test.rs"]
mod test;

use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use crate::table::bp_tree;
use crate::table::cache::Cache;
use crate::table::chunk;
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
    config: TableConfig,
    schema: TableSchema,
) -> Result<Table<F>, Error> {
    let mut metadata = TableMetadataProto::new();
    metadata.config = MessageField::some(config.clone());
    metadata.schema = MessageField::some(schema);
    metadata.root_chunk_offset = 1;
    metadata.next_chunk_offset = 2;
    {
        let mut metadata_chunk = ChunkProto::new();
        metadata_chunk.set_metadata(metadata.clone());
        chunk::write_chunk_at::<F>(&config, &mut file, metadata_chunk, 0)?;
    }
    {
        let mut root_node_chunk = ChunkProto::new();
        let root_node: &mut NodeProto = root_node_chunk.mut_node();
        root_node.offset = 1;
        root_node.set_internal(InternalNodeProto::new());
        chunk::write_chunk_at::<F>(&config, &mut file, root_node_chunk, 1)?;
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
    let mut metadata_chunk = ChunkProto::new();
    metadata_chunk.set_metadata(table.metadata.clone());
    chunk::write_chunk_at::<F>(
        &table.metadata.config,
        &mut table.file,
        metadata_chunk.clone(),
        0,
    )?;
    Ok(())
}

pub(crate) fn is_table_keyed_on_column<F: Filelike>(
    table: &Table<F>,
    column: &ColumnProto,
) -> bool {
    schema::is_schema_keyed_on_column(&table.metadata.schema, column)
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
