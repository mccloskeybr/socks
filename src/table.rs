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
use std::sync::Arc;
use tokio::sync::Mutex;

// Table file format:
// Chunk 0:          Metadata chunk
// Chunks 1 - n:     RowData directory chunks
// Chunks n+1 - end: RowData chunks

pub(crate) struct Table<F: Filelike> {
    pub(crate) file: Arc<Mutex<F>>,
    pub(crate) metadata: TableMetadataProto,
}

impl<F: Filelike> Table<F> {
    pub(crate) fn next_chunk_offset(&mut self) -> u32 {
        let offset = self.metadata.next_chunk_offset;
        self.metadata.next_chunk_offset += 1;
        log::trace!("next_chunk_offset {} <<<<<<<<<<<<<<<<<<<<<<<<<", offset);
        offset
    }

    pub(crate) fn is_table_keyed_on_column(&self, col_name: &str) -> bool {
        &self.metadata.schema.key.name == col_name
    }

    pub(crate) async fn commit_metadata(&mut self) -> Result<(), Error> {
        log::trace!("Committing metadata.");
        Buffer::new_for_file(self.file.clone(), 0, self.metadata.clone())
            .write_to_table()
            .await?;
        Ok(())
    }

    pub(crate) async fn create(
        file: F,
        name: String,
        id: u32,
        schema: TableSchema,
    ) -> Result<Self, Error> {
        let file = Arc::new(Mutex::new(file));
        let mut metadata = TableMetadataProto::new();
        metadata.name = name;
        metadata.id = id;
        metadata.schema = MessageField::some(schema);
        metadata.root_chunk_offset = 1;
        metadata.next_chunk_offset = 2;
        Buffer::new_for_file(file.clone(), 0, metadata.clone())
            .write_to_table()
            .await?;

        let mut root_node = NodeProto::new();
        root_node.offset = 1;
        root_node.set_internal(InternalNodeProto::new());
        Buffer::new_for_file(file.clone(), 1, root_node)
            .write_to_table()
            .await?;

        Ok(Self {
            file: file,
            metadata: metadata,
        })
    }

    pub(crate) async fn insert(
        &mut self,
        buffer_pool: &mut BufferPool<F>,
        key: u32,
        row: InternalRowProto,
    ) -> Result<(), Error> {
        log::trace!("Inserting row: {row}");
        bp_tree::insert(self, buffer_pool, key, row).await
    }

    pub(crate) async fn read_row(
        &mut self,
        buffer_pool: &mut BufferPool<F>,
        key: u32,
    ) -> Result<RowProto, Error> {
        log::trace!("Retrieving row with key: {key}");
        let internal_row =
            bp_tree::read_row(self, buffer_pool, self.metadata.root_chunk_offset, key).await?;
        Ok(schema::internal_row_to_row(
            &internal_row,
            &self.metadata.schema,
        ))
    }
}
