use crate::error::*;
use crate::file::*;
use crate::index::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use protobuf::text_format::parse_from_str;
use std::io::Cursor;

struct TestContext {
    file: std::io::Cursor<Vec<u8>>,
    index_config: IndexConfig,
    db_config: DatabaseConfig,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: std::io::Cursor::<Vec<u8>>::new(Vec::new()),
        index_config: parse_from_str::<IndexConfig>(
            "
            insert_method: AGGRESSIVE_SPLIT
            schema {
                name: \"TestIndex\"
                columns {
                    name: \"Key\"
                    type: INTEGER
                    is_key: true
                }
            }",
        )
        .unwrap(),
        db_config: parse_from_str::<DatabaseConfig>(
            "
            file {
                chunk_size: 512
                chunk_overflow_size: 10
            }",
        )
        .unwrap(),
    }
}

#[test]
fn find_chunk_offset_success() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.db_config, context.index_config)?;
    for i in 0..60 {
        index.insert(parse_from_str::<InsertProto>(
            "
            index_name: \"TestIndex\"
            column_values {
                name: \"Key\"
                int_value: 1
            }",
        )?)?;
    }
    assert_eq!(
        index.file.get_ref().len(),
        (index.db_config.file.chunk_size * 4) as usize
    );

    let arbitrary_data_chunk = chunk::read_chunk_at(&index.db_config.file, &mut index.file, 4)?;
    let arbitrary_data_chunk_offset =
        directory::find_chunk_offset(&mut index, arbitrary_data_chunk.node().id)?;
    assert_eq!(arbitrary_data_chunk_offset, 2);

    Ok(())
}

#[test]
fn find_chunk_offset_not_found() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.db_config, context.index_config)?;

    let Err(Error::NotFound(..)) = directory::find_chunk_offset(&mut index, 100) else {
        return Err(Error::Internal("Chunk unexpectedly found.".into()));
    };
    Ok(())
}
