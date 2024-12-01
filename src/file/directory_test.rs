use crate::index::*;
use crate::file::*;
use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::schema::*;
use protobuf::text_format::parse_from_str;
use std::io::Cursor;

struct TestContext {
    file: std::io::Cursor<Vec<u8>>,
    schema: IndexSchema,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    let schema = parse_from_str::<IndexSchema>("
        name: \"TestIndex\"
        columns {
            name: \"Key\"
            type: INTEGER
            is_key: true
        }").unwrap();
    TestContext {
        file: std::io::Cursor::<Vec<u8>>::new(Vec::new()),
        schema: schema,
    }
}

#[test]
fn find_chunk_offset_success() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.schema.clone())?;
    for i in 0..60 {
        index.insert(parse_from_str::<Insert>("
            index_name: \"TestIndex\"
            column_values {
                name: \"Key\"
                int_value: 1
            }")?)?;
    }
    assert_eq!(index.file.get_ref().len(), chunk::CHUNK_SIZE * 5);

    let arbitrary_data_chunk = chunk::read_chunk_at(&mut index.file, 4)?;
    let arbitrary_data_chunk_offset = directory::find_chunk_offset(&mut index, arbitrary_data_chunk.data().id)?;
    assert_eq!(arbitrary_data_chunk_offset, 4);

    Ok(())
}

#[test]
fn find_chunk_offset_not_found() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.schema.clone())?;

    let Err(Error::NotFound(..)) = directory::find_chunk_offset(&mut index, 100) else {
        return Err(Error::Internal("Chunk unexpectedly found.".into()));
    };
    Ok(())
}

// TODO: unit tests for other functions -- likely easier once abstractions are simplified.
