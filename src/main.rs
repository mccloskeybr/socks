#![allow(warnings)]

mod b_tree;
mod error;
mod file;
mod index;
mod parse;
mod protos;
mod stats;

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use clap::{Parser, Subcommand};
use protobuf::MessageField;
use protobuf::text_format;
use protos::generated::operations::*;
use protos::generated::schema::*;
use protos::generated::chunk::*;
use index::Index;

use std::io::Read;

use crate::error::*;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    op: Operation,
}

#[derive(Subcommand, Debug)]
enum Operation {
    Create {
        #[arg(short, long)]
        db_path: PathBuf,
        #[arg(short, long)]
        schema_path: PathBuf,
        #[arg(long)]
        overwrite: bool,
    },
    Execute {
        #[arg(short, long)]
        db_path: PathBuf,
        #[arg(short, long)]
        ops_path: PathBuf,
    },
}

fn create(op: &Operation) -> Result<(), Error> {
    let Operation::Create{
        db_path,
        schema_path,
        overwrite,
    } = op else { panic!("Unexpected arguments passed to create!"); };

    if fs::exists(db_path)? && !overwrite {
        return Err(Error::AlreadyExists("Database file already exists!".into()));
    }
    let mut db_file = fs::File::create(db_path)?;
    let schema = text_format::parse_from_str::<DatabaseSchema>(
            &fs::read_to_string(schema_path)?)?;
    Index::create(&mut db_file, schema.index.unwrap())?;

    Ok(())
}

fn execute(op: &Operation) -> Result<(), Error> {
    let Operation::Execute{
        db_path,
        ops_path,
    } = op else { panic!("Unexpected arguments passed to execute!"); };

    let operation_list =
        text_format::parse_from_str::<OperationListProto>(
            &fs::read_to_string(ops_path)?)?;
    let mut db_file = fs::File::open(db_path)?;

    todo!();

    Ok(())
}

fn main() -> Result<(), Error> {
    let args = Args::parse();
    match args.op {
        Operation::Create{ .. } => create(&args.op)?,
        Operation::Execute{ .. } => execute(&args.op)?,
    }
    Ok(())
}
