use crate::error::*;
use crate::protos::generated::chunk::*;
use crate::protos::generated::config::*;
use crate::protos::generated::operations::*;
use crate::table::parse::transform::*;
use protobuf::text_format::parse_from_str;

fn setup() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[test]
fn insert_op_success() -> Result<(), Error> {
    setup();
    let schema = parse_from_str::<TableSchema>(
        "
        name: \"TestTable\"
        columns {
            name: \"Key\"
            type: INTEGER
            is_key: true
        }
        columns {
            name: \"Col\"
            type: INTEGER
        }",
    )
    .unwrap();
    let op = parse_from_str::<InsertProto>(
        "
        table_name: \"TestTable\"
        column_values {
            name: \"Key\"
            int_value: 1
        }
        column_values {
            name: \"Col\"
            int_value: 2
        }",
    )
    .unwrap();

    let row = insert_op(op, &schema);
    assert_eq!(
        row,
        (
            1,
            parse_from_str::<InternalRowProto>(
                "
                col_values { int_value: 1 }
                col_values { int_value: 2 }"
            )
            .unwrap()
        )
    );
    Ok(())
}
