use crate::database::Database;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::operations::*;
use crate::query::QueryStage;
use crate::schema;
use crate::table::table;
use crate::table::table::Table;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::rc::Rc;

pub struct Intersect<'a, F: Filelike> {
    output_file: F,
    dependency_1: Box<dyn QueryStage + 'a>,
    dependency_2: Box<dyn QueryStage + 'a>,
}

impl<'a, F: Filelike> Intersect<'a, F> {
    pub fn new(
        path: &str,
        dependency_1: Box<dyn QueryStage + 'a>,
        dependency_2: Box<dyn QueryStage + 'a>,
    ) -> Result<Self, Error> {
        Ok(Self {
            output_file: F::create(path)?,
            dependency_1: dependency_1,
            dependency_2: dependency_2,
        })
    }
}

impl<'a, F: Filelike> QueryStage for Intersect<'a, F> {
    fn execute(&self) -> Result<(), Error> {
        Ok(())
    }
}
