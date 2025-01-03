use crate::database::*;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;
use crate::query;
use crate::query::{reader::ResultsReader, writer::ResultsWriter};
use crate::table::Table;
use std::sync::Arc;

pub(crate) async fn execute_select<F: Filelike>(
    db: &Database<F>,
    select: SelectProto,
) -> Result<F, Error> {
    let mut out = ResultsWriter::new(F::create("TODO").await?);
    let mut dep = ResultsReader::new(query::execute_query(db, select.dep.unwrap()).await?);
    let table: Arc<Table<F>> = db.table.clone();
    while let Ok(key) = dep.next_key().await {
        let row = table.read_row(key).await?;
        out.write_key_row(key, row).await?;
    }
    out.finish().await
}
