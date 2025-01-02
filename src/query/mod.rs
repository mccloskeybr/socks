use crate::database::*;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;

mod filter;
mod intersect;
mod reader;
mod select;
mod writer;

pub async fn execute_query<F: Filelike>(db: &Database<F>, query: QueryProto) -> Result<F, Error> {
    let output = match query.stage_type {
        Some(query_proto::Stage_type::Intersect(op)) => {
            Box::pin(intersect::execute_intersect(db, op)).await?
        }
        Some(query_proto::Stage_type::Filter(op)) => {
            Box::pin(filter::execute_filter(db, op)).await?
        }
        Some(query_proto::Stage_type::Select(op)) => {
            Box::pin(select::execute_select(db, op)).await?
        }
        None => panic!(),
    };
    Ok(output)
}
