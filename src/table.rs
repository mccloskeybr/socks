#[cfg(test)]
#[path = "./table_test.rs"]
mod test;

use crate::bp_tree;
use crate::buffer::Buffer;
use crate::buffer_pool::BufferPool;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use protobuf::MessageField;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

// Table file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub(crate) struct Table<F: Filelike> {
    pub(crate) file: Arc<Mutex<F>>,
    pub(crate) buffer_pool: Arc<BufferPool<F>>,
    pub(crate) name: String,
    pub(crate) id: u32,
    pub(crate) schema: TableSchema,
    pub(crate) root_chunk_offset: u32,
    pub(crate) next_chunk_offset: AtomicU32,
}

impl<F: Filelike> Table<F> {
    pub(crate) fn next_chunk_offset(&self) -> u32 {
        self.next_chunk_offset.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn is_table_keyed_on_column(&self, col_name: &str) -> bool {
        &self.schema.key.name == col_name
    }

    pub(crate) async fn commit_metadata(&self) -> Result<(), Error> {
        log::trace!("Committing metadata.");
        let mut metadata = TableMetadataProto::new();
        metadata.name = self.name.clone();
        metadata.id = self.id;
        metadata.schema = MessageField::some(self.schema.clone());
        metadata.root_chunk_offset = self.root_chunk_offset;
        metadata.next_chunk_offset = self.next_chunk_offset.load(Ordering::Relaxed);
        Buffer::new_for_file(self.file.clone(), 0, metadata)
            .write_to_file()
            .await?;
        Ok(())
    }

    pub(crate) async fn create(
        file: F,
        buffer_pool: Arc<BufferPool<F>>,
        name: String,
        id: u32,
        schema: TableSchema,
    ) -> Result<Self, Error> {
        let file = Arc::new(Mutex::new(file));
        {
            let mut metadata = TableMetadataProto::new();
            metadata.name = name.clone();
            metadata.id = id;
            metadata.schema = MessageField::some(schema.clone());
            metadata.root_chunk_offset = 1;
            metadata.next_chunk_offset = 2;
            Buffer::new_for_file(file.clone(), 0, metadata.clone())
                .write_to_file()
                .await?;
        }
        {
            let mut root_node = NodeProto::new();
            root_node.offset = 1;
            root_node.set_internal(InternalNodeProto::new());
            Buffer::new_for_file(file.clone(), 1, root_node)
                .write_to_file()
                .await?;
        }
        Ok(Self {
            file: file,
            buffer_pool: buffer_pool,
            name: name,
            id: id,
            schema: schema,
            root_chunk_offset: 1,
            next_chunk_offset: AtomicU32::new(2),
        })
    }

    pub(crate) async fn insert(&self, key: u32, row: InternalRowProto) -> Result<(), Error> {
        log::trace!("Inserting row: {row}");
        bp_tree::insert(self, key, row).await
    }

    pub(crate) async fn read_row(&self, key: u32) -> Result<RowProto, Error> {
        log::trace!("Retrieving row with key: {key}");
        let internal_row = bp_tree::read_row(self, self.root_chunk_offset, key).await?;
        Ok(schema::internal_row_to_row(&internal_row, &self.schema))
    }
}
