use crate::database;
use crate::database::Database;
use crate::error::*;
use crate::filelike::Filelike;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::schema;
use crate::table::chunk;
use crate::table::table;
use crate::table::table::Table;
use crate::LANE_WIDTH;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;
use std::simd::cmp::SimdPartialEq;
use std::simd::Simd;

// NOTE: any saved files are expected to be sorted by primary key.

// TODO: SIMD iteration
struct QueryResultsIterator<F: Filelike> {
    file: F,
    config: TableConfig,
    current_chunk: InternalQueryResultsProto,
    chunk_offset: u32,
    idx: usize,
}

impl<F: Filelike> QueryResultsIterator<F> {
    fn new(mut file: F, config: TableConfig) -> Result<Self, Error> {
        let mut chunk = chunk::read_chunk_at(&config, &mut file, 0)?;
        Ok(Self {
            file: file,
            config: config,
            current_chunk: chunk.take_query_results(),
            chunk_offset: 0,
            idx: std::usize::MAX,
        })
    }

    fn next(&mut self) -> Result<u32, Error> {
        self.idx.wrapping_add(1);
        if self.idx > self.current_chunk.keys.len() {
            self.idx = 0;
            self.chunk_offset += 1;
            let mut chunk = chunk::read_chunk_at(&self.config, &mut self.file, self.chunk_offset)?;
            self.current_chunk = chunk.take_query_results();
        }
        Ok(self.current_chunk.keys[self.idx])
    }
}

fn execute_intersect<F: Filelike>(
    db: &mut Database<F>,
    intersect: IntersectProto,
) -> Result<F, Error> {
    let config: TableConfig = db.table.borrow().metadata.config.clone().unwrap();

    let mut output = F::create("TODO")?;
    let mut chunk = ChunkProto::new();
    let results = &mut chunk.mut_query_results();

    let mut lhs_results: F = execute_query(db, intersect.lhs.unwrap())?;
    let mut rhs_results: F = execute_query(db, intersect.rhs.unwrap())?;
    let mut lhs_it = QueryResultsIterator::new(lhs_results, config.clone())?;
    let mut rhs_it = QueryResultsIterator::new(rhs_results, config.clone())?;
    let mut lhs = lhs_it.next()?;
    let mut rhs = rhs_it.next()?;
    loop {
        let ord = lhs.cmp(&rhs);
        match ord {
            Ordering::Less => {
                let Ok(next_lhs) = lhs_it.next() else {
                    break;
                };
                lhs = next_lhs;
            }
            Ordering::Greater => {
                let Ok(next_rhs) = rhs_it.next() else {
                    break;
                };
                rhs = next_rhs;
            }
            Ordering::Equal => {
                results.keys.push(lhs);

                let Ok(next_lhs) = lhs_it.next() else {
                    break;
                };
                lhs = next_lhs;
                let Ok(next_rhs) = rhs_it.next() else {
                    break;
                };
                rhs = next_rhs;
            }
        }
    }

    chunk::write_chunk_at(&config, &mut output, chunk, 0)?;
    Ok(output)
}

fn execute_filter<F: Filelike>(db: &mut Database<F>, filter: FilterProto) -> Result<F, Error> {
    let mut output = F::create("TODO")?;
    match filter.filter_type {
        Some(filter_proto::Filter_type::ColumnEquals(column)) => {
            let table: Rc<RefCell<Table<F>>> = database::find_table_keyed_on_column(db, &column)?;
            log::trace!(
                "Filtering on column: {} in table keyed by: {}",
                column.name,
                table.borrow().metadata.schema.key.name
            );

            let key = schema::get_hashed_col_value(&column);
            let row = table::read_row(&mut table.borrow_mut(), key)?;
            let pk = schema::get_col(&row, &db.table.borrow().metadata.schema.key.name);
            let pk_hash = schema::get_hashed_col_value(&pk);

            let mut chunk = ChunkProto::new();
            let results = &mut chunk.mut_query_results();
            results.keys.push(pk_hash);

            // TODO: centralize data config instead of accessing like this.
            chunk::write_chunk_at(
                db.table.borrow().metadata.config.as_ref().unwrap(),
                &mut output,
                chunk,
                0,
            )?;
        }
        None => panic!(),
    }
    Ok(output)
}

fn execute_lookup<F: Filelike>(db: &mut Database<F>, lookup: LookupProto) -> Result<F, Error> {
    let config: TableConfig = db.table.borrow().metadata.config.clone().unwrap();

    let mut dep = execute_query(db, lookup.dep.unwrap())?;
    let mut dep_results = chunk::read_chunk_at(&config, &mut dep, 0)?;
    let mut dep_results = dep_results.take_query_results();

    let mut output = F::create("TODO")?;
    let mut out_chunk = ChunkProto::new();
    let out_results = &mut out_chunk.mut_query_results();

    let table: Rc<RefCell<Table<F>>> = db.table.clone();
    for key in dep_results.keys {
        let row = table::read_row(&mut table.borrow_mut(), key)?;
        out_results.keys.push(key);
        out_results.rows.push(row);
    }

    chunk::write_chunk_at(
        db.table.borrow().metadata.config.as_ref().unwrap(),
        &mut output,
        out_chunk,
        0,
    )?;
    Ok(output)
}

pub fn execute_query<F: Filelike>(db: &mut Database<F>, query: QueryProto) -> Result<F, Error> {
    let output = match query.stage_type {
        Some(query_proto::Stage_type::Intersect(intersect)) => execute_intersect(db, intersect)?,
        Some(query_proto::Stage_type::Filter(filter)) => execute_filter(db, filter)?,
        Some(query_proto::Stage_type::Lookup(lookup)) => execute_lookup(db, lookup)?,
        None => panic!(),
    };
    Ok(output)
}
