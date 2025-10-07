mod base64;
mod types;
pub use types::*;

#[test]
fn test_equality_of_value_records() {
    let a = ValueRecord::Int { i: 0, type_id: TypeId(0) }; // just an example type_id
    let b = ValueRecord::Int { i: 0, type_id: TypeId(0) };
    let different = ValueRecord::Int { i: 1, type_id: TypeId(0) };

    assert_eq!(a, b);
    assert_ne!(a, different);
}
