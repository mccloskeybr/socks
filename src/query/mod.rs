use crate::database::Database;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;

mod filter;
mod intersect;

// go the oo route of having some kind of "is_ready" method?
//   - executor iterates through and pauses on "is_ready" check
//   - is_ready check waits for the presence of the output files from prev. dep
trait QueryStage {
    fn execute(&self) -> Result<(), Error>;
}

pub fn build_plan<'a, F: Filelike + 'a>(
    db: &mut Database<F>,
    stage: QueryStageProto,
) -> Result<Box<dyn QueryStage + 'a>, Error> {
    match stage.stage_type {
        Some(query_stage_proto::Stage_type::Intersect(intersect_proto)) => {
            let dep_1 = build_plan(db, intersect_proto.dependency_1.unwrap())?;
            let dep_2 = build_plan(db, intersect_proto.dependency_2.unwrap())?;
            let intersect = Box::new(intersect::Intersect::<F>::new("TODO", dep_1, dep_2)?);
            return Ok(intersect);
        }
        Some(query_stage_proto::Stage_type::Filter(filter_proto)) => {
            let filter = Box::new(filter::Filter::<F>::new(db, filter_proto, "TODO")?);
            return Ok(filter);
        }
        None => todo!(),
    };
}

pub fn execute_query<F: Filelike>(db: &mut Database<F>, query: QueryProto) -> Result<(), Error> {
    let plan = build_plan(db, query.input.unwrap());
    todo!();
}
