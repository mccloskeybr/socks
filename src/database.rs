#[cfg(test)]
#[path = "./database_test.rs"]
mod test;

use crate::buffer_pool::BufferPool;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::query;
use crate::schema;
use crate::table::Table;
use std::sync::Arc;

pub struct Database<F: Filelike> {
    pub(crate) table: Arc<Table<F>>,
    pub(crate) secondary_indexes: Vec<Arc<Table<F>>>,
}

impl<F: Filelike> Database<F> {
    pub(crate) fn find_table_keyed_on_column(
        &self,
        col_name: &str,
    ) -> Result<Arc<Table<F>>, Error> {
        if self.table.is_table_keyed_on_column(col_name) {
            return Ok(self.table.clone());
        }
        for secondary_index in &self.secondary_indexes {
            if secondary_index.is_table_keyed_on_column(&col_name) {
                return Ok(secondary_index.clone());
            }
        }
        return Err(Error::NotFound(format!(
            "Column not indexed: {}!",
            col_name
        )));
    }

    // TODO: validate dir doesn't exist, schema.
    // probably want to move validations to the db level instead of the index level.
    pub async fn create(dir: &str, schema: DatabaseSchema) -> Result<Self, Error> {
        let buffer_pool = Arc::new(BufferPool::new());

        let mut next_table_id = 0;
        let table = Arc::new(
            Table::create(
                F::create(format!("{}/{}", dir, "table").as_str()).await?,
                buffer_pool.clone(),
                format!("Table{}", schema.table.key.name),
                next_table_id,
                schema.table.clone().unwrap(),
            )
            .await?,
        );
        next_table_id += 1;

        let mut secondary_indexes = Vec::<Arc<Table<F>>>::new();
        for secondary_index_schema in schema.secondary_indexes {
            secondary_indexes.push(Arc::new(
                Table::create(
                    F::create(format!("{}/{}", dir, &secondary_index_schema.key.name).as_str())
                        .await?,
                    buffer_pool.clone(),
                    format!(
                        "Table{}Index{}",
                        schema.table.key.name, secondary_index_schema.key.name
                    ),
                    next_table_id,
                    schema::create_table_schema_for_index(
                        &secondary_index_schema,
                        &schema.table.as_ref().unwrap(),
                    ),
                )
                .await?,
            ));
            next_table_id += 1;
        }

        Ok(Self {
            table: table,
            secondary_indexes: secondary_indexes,
        })
    }

    // TODO: parallel insertion, failure recovery.
    // can't spawn tasks as-is due to lifetime constraints, consider the
    // actor paradigm https://ryhl.io/blog/actors-with-tokio/
    pub async fn insert(&self, op: InsertProto) -> Result<(), Error> {
        let table_key = schema::get_hashed_key_from_row(&op.row, &self.table.schema);
        let table_row_internal = schema::row_to_internal_row(&op.row);
        self.table.insert(table_key, table_row_internal).await?;

        for secondary_index in &self.secondary_indexes {
            let index_row = schema::table_row_to_index_row(
                &op.row,
                &secondary_index.schema,
                &self.table.schema,
            );
            let index_key = schema::get_hashed_key_from_row(&index_row, &secondary_index.schema);
            let index_row_internal = schema::row_to_internal_row(&index_row);
            secondary_index
                .insert(index_key, index_row_internal)
                .await?;
        }

        Ok(())
    }

    pub async fn read_row(&self, op: ReadRowProto) -> Result<RowProto, Error> {
        let hashed_key = schema::get_hashed_col_value(&op.column.value);
        self.table.read_row(hashed_key).await
    }

    pub async fn query(&self, op: QueryProto) -> Result<F, Error> {
        query::execute_query::<F>(self, op).await
    }
}
