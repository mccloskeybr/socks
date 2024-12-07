use crate::error::*;
use crate::parse::transform::*;
use crate::protos::generated::operations::*;
use crate::protos::generated::config::*;
use crate::protos::generated::chunk::*;
use protobuf::text_format::parse_from_str;

fn setup() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[test]
fn insert_op_success() -> Result<(), Error> {
    setup();
    let schema = parse_from_str::<IndexSchema>("
        name: \"TestIndex\"
        columns {
            name: \"Key\"
            type: INTEGER
            is_key: true
        }
        columns {
            name: \"Col\"
            type: INTEGER
        }").unwrap();
    let op = parse_from_str::<InsertProto>("
        index_name: \"TestIndex\"
        column_values {
            name: \"Key\"
            int_value: 1
        }
        column_values {
            name: \"Col\"
            int_value: 2
        }").unwrap();

    let row = insert_op(op, &schema);
    assert_eq!(
        row,
        parse_from_str::<InternalRowProto>("
            key: \"1.\"
            col_values { int_value: 1 }
            col_values { int_value: 2 }").unwrap());
    Ok(())
}
