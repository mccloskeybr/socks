use crate::database::*;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;

mod filter;
mod intersect;
mod reader;
mod select;
mod writer;

pub fn execute_query<F: Filelike>(db: &mut Database<F>, query: QueryProto) -> Result<F, Error> {
    let output = match query.stage_type {
        Some(query_proto::Stage_type::Intersect(op)) => intersect::execute_intersect(db, op)?,
        Some(query_proto::Stage_type::Filter(op)) => filter::execute_filter(db, op)?,
        Some(query_proto::Stage_type::Select(op)) => select::execute_select(db, op)?,
        None => panic!(),
    };
    Ok(output)
}
