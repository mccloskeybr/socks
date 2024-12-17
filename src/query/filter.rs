use crate::database::Database;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;
use crate::query::QueryStage;
use crate::schema;
use crate::table::table;
use crate::table::table::Table;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::rc::Rc;

pub struct Filter<F: Filelike> {
    table: Rc<RefCell<Table<F>>>,
    filter_proto: FilterProto, // TODO: extract value
    output_file: F,
}

fn find_table_indexed_on_column<F: Filelike>(
    db: &Database<F>,
    col: ColumnProto,
) -> Result<Rc<RefCell<Table<F>>>, Error> {
    if col.name == schema::get_primary_key(&db.table.borrow().metadata.schema).name {
        return Ok(db.table.clone());
    }
    for secondary_index in &db.secondary_indexes {
        if col.name == schema::get_primary_key(&secondary_index.borrow().metadata.schema).name {
            return Ok(secondary_index.clone());
        }
    }
    Err(Error::NotFound(format!(
        "Column not found or not indexed: {}!",
        col.name
    )))?
}

impl<F: Filelike> Filter<F> {
    pub fn new(db: &Database<F>, filter_proto: FilterProto, path: &str) -> Result<Self, Error> {
        Ok(Self {
            table: find_table_indexed_on_column(db, filter_proto.column_equals().to_owned())?,
            filter_proto: filter_proto,
            output_file: F::create(path)?,
        })
    }
}

impl<F: Filelike> QueryStage for Filter<F> {
    fn execute(&self) -> Result<(), Error> {
        let key = schema::get_hashed_col_value(self.filter_proto.column_equals());
        let internal_row = table::read_row(&mut self.table.borrow_mut(), key)?;
        Ok(())
    }
}
