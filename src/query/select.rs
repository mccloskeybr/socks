use crate::database::*;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;
use crate::query;
use crate::query::{reader::ResultsReader, writer::ResultsWriter};
use crate::table::Table;
use std::cell::RefCell;
use std::rc::Rc;

pub(crate) async fn execute_select<F: Filelike>(
    db: &mut Database<F>,
    select: SelectProto,
) -> Result<F, Error> {
    let mut out = ResultsWriter::new(F::create("TODO").await?);
    let mut dep = ResultsReader::new(query::execute_query(db, select.dep.unwrap()).await?);
    let table: Rc<RefCell<Table<F>>> = db.table.clone();
    while let Ok(key) = dep.next_key().await {
        let row = table
            .borrow_mut()
            .read_row(&mut db.buffer_pool, key)
            .await?;
        out.write_key_row(key, row).await?;
    }
    out.finish().await
}
