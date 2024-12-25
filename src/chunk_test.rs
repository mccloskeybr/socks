use crate::chunk::*;
use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::CHUNK_SIZE;
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

    let mut chunk_in = TableMetadataProto::new();
    chunk_in.next_chunk_offset = 1;
    write_chunk_at(&mut context.file, chunk_in.clone(), 0)?;
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE);

    let chunk_out: TableMetadataProto = read_chunk_at(&mut context.file, 0)?;
    assert_eq!(chunk_out, chunk_in);

    Ok(())
}

#[test]
fn read_write_many_chunks() -> Result<(), Error> {
    let mut context = setup();
    let n = 5;
    let mut chunks: Vec<TableMetadataProto> = Vec::new();
    for i in 0..n {
        let mut chunk = TableMetadataProto::new();
        chunk.next_chunk_offset = i;
        chunks.push(chunk);
    }

    for i in 0..n {
        write_chunk_at(&mut context.file, chunks[i as usize].clone(), i)?;
    }
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE * (n as usize));

    for i in 0..n {
        let chunk: TableMetadataProto = read_chunk_at(&mut context.file, i)?;
        assert_eq!(chunk, chunks[i as usize]);
    }

    Ok(())
}

#[test]
fn overwrite_chunk() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk_1 = TableMetadataProto::new();
    chunk_1.next_chunk_offset = 1;
    write_chunk_at(&mut context.file, chunk_1.clone(), 0)?;
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE);

    let mut chunk_2 = TableMetadataProto::new();
    chunk_2.next_chunk_offset = 2;
    write_chunk_at(&mut context.file, chunk_2.clone(), 0)?;
    assert_eq!(context.file.get_ref().len(), CHUNK_SIZE as usize);

    let chunk: TableMetadataProto = read_chunk_at(&mut context.file, 0)?;
    assert_eq!(chunk, chunk_2);

    Ok(())
}

#[test]
fn huge_chunk_fails_write() -> Result<(), Error> {
    let mut context = setup();

    let mut chunk = NodeProto::new();
    for i in 0..CHUNK_SIZE as usize {
        chunk.mut_internal().keys.push(std::u32::MAX);
    }
    assert!(chunk.compute_size() > CHUNK_SIZE as usize as u64);
    match write_chunk_at(&mut context.file, chunk.clone(), 0) {
        Err(Error::OutOfBounds(..)) => return Ok(()),
        _ => panic!(),
    }
}

#[test]
fn would_chunk_overflow_false() -> Result<(), Error> {
    let mut context = setup();
    assert!(!would_chunk_overflow(0));
    Ok(())
}

#[test]
fn would_chunk_overflow_true() -> Result<(), Error> {
    let mut context = setup();
    assert!(would_chunk_overflow(CHUNK_SIZE));
    Ok(())
}
