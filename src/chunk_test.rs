use crate::chunk::*;
use crate::error::*;
use crate::protos::generated::chunk::*;
use protobuf::text_format::parse_from_str;

struct TestContext {
    file: std::io::Cursor<Vec<u8>>,
}

fn setup() -> TestContext {
    let _ = env_logger::builder().is_test(true).try_init();
    TestContext {
        file: std::io::Cursor::<Vec<u8>>::new(Vec::new()),
    }
}

#[test]
fn read_write_single_chunk() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk_in = Chunk::new();
    chunk_in.mut_metadata().next_chunk_id = 1;
    write_chunk_at(&mut context.file, &chunk_in, 0)?;
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE);

    let chunk_out = read_chunk_at(&mut context.file, 0)?;
    assert_eq!(chunk_out, chunk_in);

    Ok(())
}

#[test]
fn read_write_many_chunks() -> Result<(), Error> {
    let mut context = setup();
    let n = 5;
    let mut chunks: Vec<Chunk> = Vec::new();
    for i in 0..n {
        let mut chunk = Chunk::new();
        chunk.mut_metadata().next_chunk_id = i;
        chunks.push(chunk);
    }

    for i in 0..n {
        write_chunk_at(&mut context.file, &chunks[i as usize], i)?;
    }
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE * n as usize);

    for i in 0..n {
        let chunk = read_chunk_at(&mut context.file, i)?;
        assert_eq!(chunk, chunks[i as usize]);
    }

    Ok(())
}

#[test]
fn overwrite_chunk() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk_1 = Chunk::new();
    chunk_1.mut_metadata().next_chunk_id = 1;
    write_chunk_at(&mut context.file, &chunk_1, 0)?;
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE);

    let mut chunk_2 = Chunk::new();
    chunk_2.mut_metadata().next_chunk_id = 2;
    write_chunk_at(&mut context.file, &chunk_2, 0)?;
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE);

    let chunk = read_chunk_at(&mut context.file, 0)?;
    assert_eq!(chunk, chunk_2);

    Ok(())
}

#[test]
fn huge_chunk_fails_write() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk = Chunk::new();
    let dir = chunk.mut_directory();
    for i in 0..CHUNK_SIZE {
        let mut entry = directory::Entry::new();
        entry.id = std::u32::MAX;
        entry.offset = std::u32::MAX;
        dir.entries.push(entry);
    }
    assert!(dir.compute_size() > CHUNK_SIZE as u64);
    match write_chunk_at(&mut context.file, &chunk, 0) {
        Err(Error::OutOfBounds(..)) => return Ok(()),
        _ => panic!(),
    }
}

#[test]
fn would_chunk_overflow_false() -> Result<(), Error> {
    let chunk = Chunk::new();
    let mut row = InternalRow::new();
    row.key = "key".into();
    assert!(!would_chunk_overflow(&chunk, &row));
    Ok(())
}

#[test]
fn would_chunk_overflow_true() -> Result<(), Error> {
    let mut chunk = Chunk::new();
    for i in 0..CHUNK_SIZE {
        let mut val = row_data_node::Value::new();
        val.mut_row_node().key = "key".into();
        chunk.mut_row_data().values.push(val);
    }
    let mut row = InternalRow::new();
    row.key = "key".into();
    assert!(would_chunk_overflow(&chunk, &row));
    Ok(())
}

#[test]
fn is_leaf_node_true() -> Result<(), Error> {
    let chunk = parse_from_str::<RowDataNode>("
            id: 0
            values {
                row_node {
                    key: \"key\"
                    col_values { int_value: 1 }
                }
            }
            values {
                row_node {
                    key: \"key\"
                    col_values { int_value: 2 }
                }
            }")?;
    assert!(is_leaf_node(&chunk));
    Ok(())
}

#[test]
fn is_leaf_node_false() -> Result<(), Error> {
    let chunk = parse_from_str::<RowDataNode>("
            id: 0
            values { child_id: 1 }
            values {
                row_node {
                    key: \"key\"
                    col_values { int_value: 2 }
                }
            }
            values { child_id: 2 }")?;
    assert!(!is_leaf_node(&chunk));
    Ok(())
}
