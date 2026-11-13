//! Structs must be writable while streaming an enclosing value, including
//! empty objects such as Ruby Procs. Encoding a separate ValueRecord resets
//! the encoder and would discard the enclosing sequence and its siblings.
//! No mocks: both paths use the production Nim encoder through its C ABI.

use codetracer_trace_types::{TypeId, ValueRecord};
use codetracer_trace_writer_nim::StreamingValueEncoder;

#[test]
fn streamed_structs_preserve_their_fields_and_enclosing_siblings() {
    let expected = ValueRecord::Sequence {
        elements: vec![
            ValueRecord::Int { i: 11, type_id: TypeId(1) },
            ValueRecord::Struct {
                field_values: vec![],
                type_id: TypeId(2),
            },
            ValueRecord::Struct {
                field_values: vec![ValueRecord::String {
                    text: "field".into(),
                    type_id: TypeId(3),
                }],
                type_id: TypeId(4),
            },
            ValueRecord::Int { i: 22, type_id: TypeId(1) },
        ],
        is_slice: false,
        type_id: TypeId(5),
    };
    let mut tree = StreamingValueEncoder::new();
    let expected_bytes = tree.encode(&expected).to_vec();
    assert_eq!(tree.take_failure(), None);

    let mut streamed = StreamingValueEncoder::new();
    streamed.begin_sequence(TypeId(5), 4);
    streamed.write_int(11, TypeId(1));
    streamed.begin_struct(TypeId(2), 0);
    streamed.end_compound();
    streamed.begin_struct(TypeId(4), 1);
    streamed.write_string("field", TypeId(3));
    streamed.end_compound();
    streamed.write_int(22, TypeId(1));
    streamed.end_compound();
    assert_eq!(streamed.take_failure(), None);
    assert_eq!(streamed.get_bytes_copy(), expected_bytes);
}
