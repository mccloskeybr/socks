#[cfg(test)]
#[path = "./table_test.rs"]
mod test;

use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::table::bp_tree;
use crate::table::cache::Cache;
use crate::table::chunk;
use crate::table::transform;
use crate::table::validate;
use protobuf::Message;
use protobuf::MessageField;
use std::io::{Read, Seek, Write};

// Table file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub(crate) struct Table<'a, F: 'a + Read + Write + Seek> {
    pub(crate) file: &'a mut F,
    pub(crate) cache: Cache,
    pub(crate) metadata: TableMetadataProto,
    pub(crate) db_config: DatabaseConfig,
}

pub(crate) fn next_chunk_offset<F: Read + Write + Seek>(table: &mut Table<F>) -> u32 {
    let offset = table.metadata.next_chunk_offset;
    table.metadata.next_chunk_offset += 1;
    offset
}

pub(crate) fn commit_metadata<F: Read + Write + Seek>(table: &mut Table<F>) -> Result<(), Error> {
    log::trace!("Committing metadata.");
    let mut metadata_chunk = ChunkProto::new();
    metadata_chunk.set_metadata(table.metadata.clone());
    chunk::write_chunk_at::<F>(
        &table.db_config.file,
        &mut table.file,
        metadata_chunk.clone(),
        0,
    )?;
    Ok(())
}

pub(crate) fn create<'a, F: Read + Write + Seek>(
    file: &'a mut F,
    db_config: DatabaseConfig,
    table_config: TableConfig,
) -> Result<Table<F>, Error> {
    validate::schema(&table_config.schema)?;

    let mut metadata = TableMetadataProto::new();
    metadata.config = MessageField::some(table_config.clone());
    metadata.root_chunk_offset = 1;
    metadata.next_chunk_offset = 2;
    {
        let mut metadata_chunk = ChunkProto::new();
        metadata_chunk.set_metadata(metadata.clone());
        chunk::write_chunk_at::<F>(&db_config.file, file, metadata_chunk, 0)?;
    }
    {
        let mut root_node_chunk = ChunkProto::new();
        let root_node: &mut NodeProto = root_node_chunk.mut_node();
        root_node.offset = 1;
        root_node.set_internal(InternalNodeProto::new());
        chunk::write_chunk_at::<F>(&db_config.file, file, root_node_chunk, 1)?;
    }

    Ok(Table {
        file: file,
        cache: Cache::default(),
        metadata: metadata,
        db_config: db_config,
    })
}

pub(crate) fn insert<F: Read + Write + Seek>(table: &mut Table<F>, op: InsertProto) -> Result<(), Error> {
    // TODO: validate op
    let (key, row) = transform::insert_op(op, &table.metadata.config.schema);
    log::trace!("Inserting row: {row}");
    bp_tree::insert(table, key, row)
}

pub(crate) fn read_row<F: Read + Write + Seek>(
    table: &mut Table<F>,
    op: ReadRowProto,
) -> Result<InternalRowProto, Error> {
    // TODO: validate op
    let key: u32 = transform::read_row_op(op, &table.metadata.config.schema);
    bp_tree::read_row(table, table.metadata.root_chunk_offset, key)
}
