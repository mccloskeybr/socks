#[cfg(test)]
#[path = "./database_test.rs"]
mod test;

use crate::cache::Cache;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::query;
use crate::schema;
use crate::table;
use crate::table::Table;
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::rc::Rc;

pub struct Database<F: Filelike> {
    pub(crate) table: Rc<RefCell<Table<F>>>,
    pub(crate) secondary_indexes: Vec<Rc<RefCell<Table<F>>>>,
    pub(crate) cache: Cache,
}

impl<F: Filelike> Database<F> {
    pub(crate) fn find_table_keyed_on_column(
        &self,
        col_name: &str,
    ) -> Result<Rc<RefCell<Table<F>>>, Error> {
        if self.table.borrow().is_table_keyed_on_column(col_name) {
            log::trace!("1");
            return Ok(self.table.clone());
        }
        for secondary_index in &self.secondary_indexes {
            if secondary_index.borrow().is_table_keyed_on_column(&col_name) {
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
    pub fn create(dir: &str, schema: DatabaseSchema) -> Result<Self, Error> {
        let mut next_table_id = 0;
        let table = Table::create(
            F::create(format!("{}/{}", dir, "table").as_str())?,
            format!("Table{}", schema.table.key.name),
            next_table_id,
            schema.table.clone().unwrap(),
        )?;
        next_table_id += 1;

        let mut secondary_indexes = Vec::<Rc<RefCell<Table<F>>>>::new();
        for secondary_index_schema in schema.secondary_indexes {
            secondary_indexes.push(Rc::new(RefCell::new(Table::create(
                F::create(format!("{}/{}", dir, &secondary_index_schema.key.name).as_str())?,
                format!(
                    "Table{}Index{}",
                    schema.table.key.name, secondary_index_schema.key.name
                ),
                next_table_id,
                schema::create_table_schema_for_index(
                    &secondary_index_schema,
                    &schema.table.as_ref().unwrap(),
                ),
            )?)));
            next_table_id += 1;
        }

        Ok(Self {
            table: Rc::new(RefCell::new(table)),
            secondary_indexes: secondary_indexes,
            cache: Cache::default(),
        })
    }

    // TODO: parallel insertion, failure recovery.
    pub fn insert(&mut self, op: InsertProto) -> Result<(), Error> {
        let table_key =
            schema::get_hashed_key_from_row(&op.row, &self.table.borrow().metadata.schema);
        let table_row_internal =
            schema::row_to_internal_row(&op.row, &self.table.borrow().metadata.schema);
        self.table
            .borrow_mut()
            .insert(&mut self.cache, table_key, table_row_internal)?;

        for secondary_index in &mut self.secondary_indexes {
            let index_row = schema::table_row_to_index_row(
                &op.row,
                &secondary_index.borrow().metadata.schema.as_ref().unwrap(),
                &self.table.borrow().metadata.schema.as_ref().unwrap(),
            );
            let index_key = schema::get_hashed_key_from_row(
                &index_row,
                &secondary_index.borrow().metadata.schema,
            );
            let index_row_internal =
                schema::row_to_internal_row(&index_row, &secondary_index.borrow().metadata.schema);
            secondary_index
                .borrow_mut()
                .insert(&mut self.cache, index_key, index_row_internal);
        }

        Ok(())
    }

    pub fn read_row(&mut self, op: ReadRowProto) -> Result<RowProto, Error> {
        let hashed_key = schema::get_hashed_col_value(&op.column.value);
        self.table
            .borrow_mut()
            .read_row(&mut self.cache, hashed_key)
    }

    pub fn query(&mut self, op: QueryProto) -> Result<F, Error> {
        query::execute_query::<F>(self, op)
    }
}
