use crate::database::Database;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;
use crate::query::writer::ResultsWriter;
use crate::schema;
use crate::table::Table;
use std::sync::Arc;

async fn execute_filter_equals<F: Filelike>(
    db: &Database<F>,
    equals: filter_proto::FilterEqualsProto,
) -> Result<F, Error> {
    let table: Arc<Table<F>> = db.find_table_keyed_on_column(&equals.name)?;
    log::trace!(
        "Filtering on column: {} in table: {}",
        equals.name,
        table.name,
    );

    // TODO: return empty on doesn't exist instead of error.
    let key = schema::get_hashed_col_value(&equals.value);
    let row = table.read_row(&db.buffer_pool, key).await?;
    let pk = schema::get_col(&row, &db.table.schema.key.name);
    let pk_hash = schema::get_hashed_col_value(&pk.value);

    let mut out = ResultsWriter::new(F::create("TODO").await?);
    out.write_key(pk_hash).await?;
    out.finish().await
}

fn execute_filter_in_range<F: Filelike>(
    _db: &Database<F>,
    _in_range: filter_proto::FilterInRangeProto,
) -> Result<F, Error> {
    // TODO: current api returns the row only, for this we need to keep state.
    todo!()
}

pub(crate) async fn execute_filter<F: Filelike>(
    db: &Database<F>,
    filter: FilterProto,
) -> Result<F, Error> {
    match filter.filter_type {
        Some(filter_proto::Filter_type::Equals(equals)) => execute_filter_equals(db, equals).await,
        Some(filter_proto::Filter_type::InRange(in_range)) => execute_filter_in_range(db, in_range),
        None => panic!(),
    }
}
