use crate::file::*;
use crate::error::*;
use crate::protos::generated::chunk::*;
use protobuf::text_format::parse_from_str;

fn setup() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[test]
fn is_leaf_node_true() -> Result<(), Error> {
    setup();
    let chunk = parse_from_str::<DataProto>("
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
    assert!(row_data::is_leaf_node(&chunk));
    Ok(())
}

#[test]
fn is_leaf_node_false() -> Result<(), Error> {
    setup();
    let chunk = parse_from_str::<DataProto>("
            id: 0
            values { child_id: 1 }
            values {
                row_node {
                    key: \"key\"
                    col_values { int_value: 2 }
                }
            }
            values { child_id: 2 }")?;
    assert!(!row_data::is_leaf_node(&chunk));
    Ok(())
}
