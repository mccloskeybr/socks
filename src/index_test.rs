use crate::index::*;
use crate::file::*;
use crate::parse::*;
use crate::protos::generated::chunk::*;
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

fn validate_node_sorted(node: &DataProto) {
    let mut max_key_seen: Option<String> = None;
    for val in &node.values {
        if val.has_child_id() {
            continue;
        } else if max_key_seen.is_none() {
            max_key_seen = Some(val.row_node().key.clone());
            continue;
        }
        assert!(max_key_seen.unwrap() <= val.row_node().key);
        max_key_seen = Some(val.row_node().key.clone());
    }
}

#[test]
fn create_success() -> Result<(), Error> {
    let mut context = setup();

    Index::create(&mut context.file, context.schema)?;
    assert_eq!(context.file.get_ref().len(), chunk::CHUNK_SIZE * 3);
    let chunk_0 = chunk::read_chunk_at(&mut context.file, 0)?;
    let chunk_1 = chunk::read_chunk_at(&mut context.file, 1)?;
    let chunk_2 = chunk::read_chunk_at(&mut context.file, 2)?;

    assert!(chunk_0.has_metadata());
    let metadata = chunk_0.metadata();
    assert_eq!(metadata.next_chunk_id, 1);
    assert_eq!(metadata.next_chunk_offset, 3);
    assert_eq!(metadata.root_chunk_id, 0);

    assert!(chunk_1.has_directory());
    let dir = chunk_1.directory();
    assert_eq!(dir.entries.len(), 1);
    assert_eq!(dir.entries[0].id, 0);
    assert_eq!(dir.entries[0].offset, 2);

    assert!(chunk_2.has_data());
    let data = chunk_2.data();
    assert_eq!(data.id, 0);
    assert_eq!(data.values.len(), 0);

    Ok(())
}

#[test]
fn open_success() -> Result<(), Error> {
    let mut context = setup();
    Index::create(&mut context.file, context.schema)?;
    Index::open(&mut context.file)?;
    Ok(())
}

#[test]
fn insert_single_value() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.schema.clone())?;

    let op = parse_from_str::<Insert>("
            index_name: \"TestIndex\"
            column_values {
                name: \"Key\"
                int_value: 1
            }")?;
    index.insert(op.clone())?;

    assert_eq!(context.file.get_ref().len(), chunk::CHUNK_SIZE * 3);
    let data_chunk = chunk::read_chunk_at(&mut context.file, 2)?;

    assert!(data_chunk.has_data());
    let data = data_chunk.data();
    assert_eq!(data.id, 0);
    assert_eq!(data.values.len(), 1);
    assert!(data.values[0].has_row_node());
    assert_eq!(*data.values[0].row_node(), transform::insert_op(op, &context.schema));

    Ok(())
}

#[test]
fn insert_some_values_sorted() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.schema.clone())?;

    let op_1 = parse_from_str::<Insert>("
            index_name: \"TestIndex\"
            column_values {
                name: \"Key\"
                int_value: 1
            }")?;
    let op_2 = parse_from_str::<Insert>("
            index_name: \"TestIndex\"
            column_values {
                name: \"Key\"
                int_value: 2
            }")?;
    let op_3 = parse_from_str::<Insert>("
            index_name: \"TestIndex\"
            column_values {
                name: \"Key\"
                int_value: 3
            }")?;
    index.insert(op_1.clone())?;
    index.insert(op_2.clone())?;
    index.insert(op_3.clone())?;

    assert_eq!(context.file.get_ref().len(), chunk::CHUNK_SIZE * 3);
    let data_chunk = chunk::read_chunk_at(&mut context.file, 2)?;

    assert!(data_chunk.has_data());
    let data = data_chunk.data();
    validate_node_sorted(data);
    assert_eq!(data.id, 0);
    assert_eq!(data.values.len(), 3);
    let row_1 = data.values[0].row_node();
    let row_2 = data.values[1].row_node();
    let row_3 = data.values[2].row_node();
    assert_eq!(*row_1, transform::insert_op(op_1, &context.schema));
    assert_eq!(*row_2, transform::insert_op(op_2, &context.schema));
    assert_eq!(*row_3, transform::insert_op(op_3, &context.schema));

    Ok(())
}

#[test]
fn insert_values_overflow() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.schema.clone())?;

    for i in 0..60 {
        let mut col_val = ColumnValue::new();
        col_val.name = "Key".into();
        col_val.set_int_value(i as i32);

        let mut op = Insert::new();
        op.index_name = "TestIndex".into();
        op.column_values.push(col_val);

        index.insert(op)?;
    }

    let metadata = chunk::read_chunk_at(&mut context.file, 0)?;
    let metadata = metadata.metadata();
    assert_eq!(metadata.next_chunk_id, 4);
    assert_eq!(metadata.next_chunk_offset, 6);
    assert_eq!(metadata.root_chunk_id, 0);
    assert_eq!(metadata.num_directories, 1);

    let dir = chunk::read_chunk_at(&mut context.file, 1)?;
    let dir = dir.directory();
    assert_eq!(dir.entries.len(), 4);
    assert_eq!(dir.entries[0].id, 0);
    assert_eq!(dir.entries[0].offset, 2);
    assert_eq!(dir.entries[1].id, 1);
    assert_eq!(dir.entries[1].offset, 3);
    assert_eq!(dir.entries[2].id, 2);
    assert_eq!(dir.entries[2].offset, 4);
    assert_eq!(dir.entries[3].id, 3);
    assert_eq!(dir.entries[3].offset, 5);

    assert_eq!(context.file.get_ref().len(), chunk::CHUNK_SIZE * 6);
    for i in 2..5 {
        let data_chunk = chunk::read_chunk_at(&mut context.file, i)?;
        validate_node_sorted(data_chunk.data());
    }

    Ok(())
}

/*
#[test]
fn insert_large_number_of_rows() -> Result<(), Error> {
    let mut context = setup();
    let mut index = Index::create(&mut context.file, context.schema.clone())?;

    for i in 0..100000 {
        let mut col_val = ColumnValue::new();
        col_val.name = "Key".into();
        col_val.set_int_value(i as i32);

        let mut op = Insert::new();
        op.index_name = "TestIndex".into();
        op.column_values.push(col_val);

        index.insert(op)?;
    }

    Ok(())
}
*/
