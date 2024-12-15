#[cfg(test)]
#[path = "./database_test.rs"]
mod test;

use crate::error::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use crate::table::table;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};

pub struct Database<F: Read + Write + Seek> {
    table: table::Table<F>,
    secondary_indexes: Vec<table::Table<F>>,
}

fn open_file(dir: &str, file_name: &str) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(format!("{}/{}", dir, file_name))?;
    Ok(file)
}

// TODO: validate dir doesn't exist, config, schema.
// probably want to move validations to the db level instead of the index level.
pub fn create(
    dir: &str,
    config: TableConfig,
    schema: DatabaseSchema,
) -> Result<Database<File>, Error> {
    let table = table::create(
        open_file(dir, "table")?,
        config.clone(),
        schema.table.clone().unwrap(),
    )?;

    let mut secondary_indexes = Vec::<table::Table<File>>::new();
    for secondary_index_schema in schema.secondary_indexes {
        secondary_indexes.push(table::create(
            open_file(dir, &secondary_index_schema.column.name)?,
            config.clone(),
            schema::create_table_schema_for_index(
                &secondary_index_schema,
                &schema.table.as_ref().unwrap(),
            ),
        )?);
    }

    Ok(Database {
        table: table,
        secondary_indexes: secondary_indexes,
    })
}

// TODO: parallel insertion, failure recovery.
pub fn insert<F: Read + Write + Seek>(db: &mut Database<F>, op: InsertProto) -> Result<(), Error> {
    let table_key = schema::get_hashed_key_from_row(&op.row, &db.table.metadata.schema);
    let table_row_internal = schema::row_to_internal_row(&op.row, &db.table.metadata.schema);
    table::insert(&mut db.table, table_key, table_row_internal)?;

    for secondary_index in &mut db.secondary_indexes {
        let index_row = schema::table_row_to_index_row(
            &op.row,
            &secondary_index.metadata.schema.as_ref().unwrap(),
            table_key,
        );
        let index_key =
            schema::get_hashed_key_from_row(&index_row, &secondary_index.metadata.schema);
        let index_row_internal =
            schema::row_to_internal_row(&index_row, &secondary_index.metadata.schema);
        table::insert(secondary_index, index_key, index_row_internal);
    }

    Ok(())
}

pub fn read_row<F: Read + Write + Seek>(
    db: &mut Database<F>,
    op: ReadRowProto,
) -> Result<RowProto, Error> {
    let hashed_key = schema::get_hashed_col_value(&op.key_value);
    let internal_row = table::read_row(&mut db.table, hashed_key)?;
    Ok(schema::internal_row_to_row(
        &internal_row,
        &db.table.metadata.schema,
    ))
}
