#[cfg(test)]
#[path = "./database_test.rs"]
mod test;

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
    pub(crate) config: TableConfig,
    pub(crate) table: Rc<RefCell<Table<F>>>,
    pub(crate) secondary_indexes: Vec<Rc<RefCell<Table<F>>>>,
}

pub(crate) fn find_table_keyed_on_column<F: Filelike>(
    db: &Database<F>,
    col_name: &str,
) -> Result<Rc<RefCell<Table<F>>>, Error> {
    if table::is_table_keyed_on_column(&db.table.borrow(), col_name) {
        log::trace!("1");
        return Ok(db.table.clone());
    }
    for secondary_index in &db.secondary_indexes {
        if table::is_table_keyed_on_column(&secondary_index.borrow(), &col_name) {
            return Ok(secondary_index.clone());
        }
    }
    return Err(Error::NotFound(format!(
        "Column not indexed: {}!",
        col_name
    )));
}

// TODO: validate dir doesn't exist, config, schema.
// probably want to move validations to the db level instead of the index level.
pub fn create<F: Filelike>(
    dir: &str,
    config: TableConfig,
    schema: DatabaseSchema,
) -> Result<Database<F>, Error> {
    let table = table::create(
        F::create(format!("{}/{}", dir, "table").as_str())?,
        format!("Table{}", schema.table.key.name),
        config.clone(),
        schema.table.clone().unwrap(),
    )?;

    let mut secondary_indexes = Vec::<Rc<RefCell<Table<F>>>>::new();
    for secondary_index_schema in schema.secondary_indexes {
        secondary_indexes.push(Rc::new(RefCell::new(table::create(
            F::create(format!("{}/{}", dir, &secondary_index_schema.key.name).as_str())?,
            format!(
                "Table{}Index{}",
                schema.table.key.name, secondary_index_schema.key.name
            ),
            config.clone(),
            schema::create_table_schema_for_index(
                &secondary_index_schema,
                &schema.table.as_ref().unwrap(),
            ),
        )?)));
    }

    Ok(Database {
        config: config,
        table: Rc::new(RefCell::new(table)),
        secondary_indexes: secondary_indexes,
    })
}

// TODO: parallel insertion, failure recovery.
pub fn insert<F: Filelike>(db: &mut Database<F>, op: InsertProto) -> Result<(), Error> {
    let table_key = schema::get_hashed_key_from_row(&op.row, &db.table.borrow().metadata.schema);
    let table_row_internal =
        schema::row_to_internal_row(&op.row, &db.table.borrow().metadata.schema);
    table::insert(&mut db.table.borrow_mut(), table_key, table_row_internal)?;

    for secondary_index in &mut db.secondary_indexes {
        let index_row = schema::table_row_to_index_row(
            &op.row,
            &secondary_index.borrow().metadata.schema.as_ref().unwrap(),
            &db.table.borrow().metadata.schema.as_ref().unwrap(),
        );
        let index_key =
            schema::get_hashed_key_from_row(&index_row, &secondary_index.borrow().metadata.schema);
        let index_row_internal =
            schema::row_to_internal_row(&index_row, &secondary_index.borrow().metadata.schema);
        table::insert(
            &mut secondary_index.borrow_mut(),
            index_key,
            index_row_internal,
        );
    }

    Ok(())
}

pub fn read_row<F: Filelike>(db: &mut Database<F>, op: ReadRowProto) -> Result<RowProto, Error> {
    let hashed_key = schema::get_hashed_col_value(&op.column.value);
    table::read_row(&mut db.table.borrow_mut(), hashed_key)
}

pub fn query<F: Filelike>(db: &mut Database<F>, op: QueryProto) -> Result<F, Error> {
    query::execute_query::<F>(db, op)
}
