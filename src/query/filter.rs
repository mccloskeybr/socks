use crate::database;
use crate::database::Database;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::query::{reader::ResultsReader, writer::ResultsWriter};
use crate::schema;
use crate::table;
use crate::table::Table;
use std::cell::RefCell;
use std::rc::Rc;

fn execute_filter_equals<F: Filelike>(
    db: &mut Database<F>,
    equals: filter_proto::FilterEqualsProto,
) -> Result<F, Error> {
    let table: Rc<RefCell<Table<F>>> = database::find_table_keyed_on_column(db, &equals.name)?;
    log::trace!(
        "Filtering on column: {} in table: {}",
        equals.name,
        table.borrow().metadata.name,
    );

    // TODO: return empty on doesn't exist instead of error.
    let key = schema::get_hashed_col_value(&equals.value);
    let row = table::read_row(&mut table.borrow_mut(), key)?;
    let pk = schema::get_col(&row, &db.table.borrow().metadata.schema.key.name);
    let pk_hash = schema::get_hashed_col_value(&pk.value);

    let mut out = ResultsWriter::new(F::create("TODO")?, db.config.clone());
    out.write_key(pk_hash)?;
    out.flush()?;
    Ok(out.file)
}

fn execute_filter_in_range<F: Filelike>(
    db: &mut Database<F>,
    in_range: filter_proto::FilterInRangeProto,
) -> Result<F, Error> {
    // TODO: current api returns the row only, for this we need to keep state.
    todo!()
}

pub(crate) fn execute_filter<F: Filelike>(
    db: &mut Database<F>,
    filter: FilterProto,
) -> Result<F, Error> {
    match filter.filter_type {
        Some(filter_proto::Filter_type::Equals(equals)) => execute_filter_equals(db, equals),
        Some(filter_proto::Filter_type::InRange(in_range)) => execute_filter_in_range(db, in_range),
        None => panic!(),
    }
}
