use crate::database::*;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;
use crate::query;
use crate::query::{reader::ResultsReader, writer::ResultsWriter};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

pub(crate) fn execute_intersect<F: Filelike>(
    db: &mut Database<F>,
    intersect: IntersectProto,
) -> Result<F, Error> {
    let mut out = ResultsWriter::new(F::create("TODO")?);

    let mut lhs_it = ResultsReader::new(query::execute_query(db, intersect.lhs.unwrap())?);
    let mut rhs_it = ResultsReader::new(query::execute_query(db, intersect.rhs.unwrap())?);
    let mut lhs = lhs_it.next_key()?;
    let mut rhs = rhs_it.next_key()?;
    loop {
        let ord = lhs.cmp(&rhs);
        match ord {
            Ordering::Less => {
                let Ok(next_lhs) = lhs_it.next_key() else {
                    break;
                };
                lhs = next_lhs;
            }
            Ordering::Greater => {
                let Ok(next_rhs) = rhs_it.next_key() else {
                    break;
                };
                rhs = next_rhs;
            }
            Ordering::Equal => {
                out.write_key(lhs)?;

                let Ok(next_lhs) = lhs_it.next_key() else {
                    break;
                };
                lhs = next_lhs;
                let Ok(next_rhs) = rhs_it.next_key() else {
                    break;
                };
                rhs = next_rhs;
            }
        }
    }
    out.flush()?;

    Ok(out.file)
}
