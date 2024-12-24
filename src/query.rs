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
use protobuf::Message;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

// NOTE: any saved files are expected to be sorted by primary key.

// TODO: SIMD iteration
struct ResultsReader<F: Filelike> {
    file: F,
    config: TableConfig,
    current_chunk: InternalQueryResultsProto,
    current_chunk_offset: u32,
    idx: usize,
}

impl<F: Filelike> ResultsReader<F> {
    fn new(mut file: F, config: TableConfig) -> Result<Self, Error> {
        Ok(Self {
            file: file,
            config: config,
            current_chunk: InternalQueryResultsProto::new(),
            current_chunk_offset: std::u32::MAX,
            idx: std::usize::MAX,
        })
    }

    fn next_key(&mut self) -> Result<u32, Error> {
        self.idx = self.idx.wrapping_add(1);
        if self.idx >= self.current_chunk.keys.len() {
            self.idx = 0;
            self.current_chunk_offset = self.current_chunk_offset.wrapping_add(1);
            dbg!("{}", self.current_chunk_offset);
            let mut chunk =
                chunk::read_chunk_at(&self.config, &mut self.file, self.current_chunk_offset)?;
            self.current_chunk = chunk.take_query_results();
            // NOTE: it seems like cursors don't OOB when reading outside written bounds?
            if self.current_chunk.keys.len() == 0 {
                return Err(Error::OutOfBounds("".to_string()));
            }
        }
        Ok(self.current_chunk.keys[self.idx])
    }
}

struct ResultsWriter<F: Filelike> {
    file: F,
    config: TableConfig,
    current_chunk: ChunkProto,
    current_chunk_offset: u32,
}

impl<F: Filelike> ResultsWriter<F> {
    fn new(mut file: F, config: TableConfig) -> Result<Self, Error> {
        Ok(Self {
            file: file,
            config: config,
            current_chunk: ChunkProto::new(),
            current_chunk_offset: 0,
        })
    }

    fn write_key(&mut self, key: u32) -> Result<(), Error> {
        if chunk::would_chunk_overflow(
            &self.config,
            self.current_chunk.compute_size() as usize + std::mem::size_of::<u32>(),
        ) {
            chunk::write_chunk_at::<F>(
                &self.config,
                &mut self.file,
                self.current_chunk.clone(),
                self.current_chunk_offset,
            )?;
            self.current_chunk_offset += 1;
            self.current_chunk = ChunkProto::new();
        }
        self.current_chunk.mut_query_results().keys.push(key);
        Ok(())
    }

    fn write_key_row(&mut self, key: u32, row: RowProto) -> Result<(), Error> {
        if chunk::would_chunk_overflow(
            &self.config,
            self.current_chunk.compute_size() as usize
                + row.compute_size() as usize
                + std::mem::size_of::<u32>(),
        ) {
            chunk::write_chunk_at::<F>(
                &self.config,
                &mut self.file,
                self.current_chunk.clone(),
                self.current_chunk_offset,
            )?;
            self.current_chunk_offset += 1;
            self.current_chunk = ChunkProto::new();
        }
        self.current_chunk.mut_query_results().keys.push(key);
        self.current_chunk.mut_query_results().rows.push(row);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        dbg!(
            "flushing: {} at {}",
            &self.current_chunk,
            self.current_chunk_offset
        );
        chunk::write_chunk_at::<F>(
            &self.config,
            &mut self.file,
            self.current_chunk.clone(),
            self.current_chunk_offset,
        )?;
        Ok(())
    }
}

fn execute_intersect<F: Filelike>(
    db: &mut Database<F>,
    intersect: IntersectProto,
) -> Result<F, Error> {
    let mut out = ResultsWriter::new(F::create("TODO")?, db.config.clone())?;

    let mut lhs_it = ResultsReader::new(
        execute_query(db, intersect.lhs.unwrap())?,
        db.config.clone(),
    )?;
    let mut rhs_it = ResultsReader::new(
        execute_query(db, intersect.rhs.unwrap())?,
        db.config.clone(),
    )?;
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

fn execute_filter<F: Filelike>(db: &mut Database<F>, filter: FilterProto) -> Result<F, Error> {
    let mut out = ResultsWriter::new(F::create("TODO")?, db.config.clone())?;
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

            out.write_key(pk_hash)?;
        }
        None => panic!(),
    }
    out.flush()?;
    Ok(out.file)
}

fn execute_lookup<F: Filelike>(db: &mut Database<F>, lookup: LookupProto) -> Result<F, Error> {
    let mut out = ResultsWriter::new(F::create("TODO")?, db.config.clone())?;
    let mut dep = ResultsReader::new(execute_query(db, lookup.dep.unwrap())?, db.config.clone())?;
    let table: Rc<RefCell<Table<F>>> = db.table.clone();
    while let Ok(key) = dep.next_key() {
        let row = table::read_row(&mut table.borrow_mut(), key)?;
        out.write_key_row(key, row)?;
    }
    out.flush()?;
    Ok(out.file)
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
