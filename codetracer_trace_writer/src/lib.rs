mod abstract_trace_writer;
mod non_streaming_trace_writer;
pub mod trace_writer;

#[cfg(target_arch = "wasm32")]
#[path = "./cbor_zstd_writer_wasm.rs"]
mod cbor_zstd_writer;

#[cfg(not(target_arch = "wasm32"))]
mod cbor_zstd_writer;

#[derive(Debug, Clone, Copy)]
pub enum TraceEventsFileFormat {
    Json,
    BinaryV0,
    Binary,
}

pub fn create_trace_writer(program: &str, args: &[String], format: TraceEventsFileFormat) -> Box<dyn trace_writer::TraceWriter> {
    match format {
        TraceEventsFileFormat::Json | TraceEventsFileFormat::BinaryV0 => {
            let mut result = Box::new(non_streaming_trace_writer::NonStreamingTraceWriter::new(program, args));
            result.set_format(format);
            result
        }
        TraceEventsFileFormat::Binary => Box::new(crate::cbor_zstd_writer::CborZstdTraceWriter::new(program, args)),
    }
}

#[cfg(test)]
mod tests {
    use codetracer_trace_types::*;
    use std::path::Path;
    use crate::{non_streaming_trace_writer::NonStreamingTraceWriter, trace_writer::TraceWriter};

    #[test]
    fn test_simple_trace() {
        let mut tracer = NonStreamingTraceWriter::new("path.small", &vec![]);
        let path = Path::new("/test/path.small");
        tracer.start(path, Line(1));
        tracer.register_step(path, Line(1));
        tracer.register_step(path, Line(2));
        tracer.register_asm(&["asm0".to_string(), "asm1".to_string()]);
        tracer.register_special_event(EventLogKind::Write, "test");
        tracer.register_special_event(EventLogKind::Write, "test2");
        tracer.register_special_event(EventLogKind::Error, "testError");

        let function_path_id = tracer.ensure_path_id(&path);
        let function_line = Line(3);
        // -> function_id 1 after top level;
        let function_id = tracer.ensure_function_id("function", &path, function_line);
        assert!(function_id == FunctionId(1));

        let before_temp_step = tracer.events.len();
        tracer.register_step(path, function_line);
        tracer.drop_last_step();
        // drop last step: drops steps[-1]/variables[-]
        assert_eq!(before_temp_step + 2, tracer.events.len());
        assert!(matches!(tracer.events.last().unwrap(), TraceLowLevelEvent::DropLastStep));

        let args = vec![tracer.arg("a", NONE_VALUE), tracer.arg("b", NONE_VALUE)];
        tracer.register_call(function_id, args);
        // => arg-related variable/value events; auto call-step event; potentially variables; call event

        assert!(tracer.events.len() > 3);
        // println!("{:#?}", tracer.events);
        // -4, -3 should be variables
        let should_be_step = &tracer.events[tracer.events.len() - 2];
        let should_be_call = &tracer.events[tracer.events.len() - 1];
        if let TraceLowLevelEvent::Step(StepRecord { path_id, line }) = should_be_step {
            assert_eq!(*path_id, function_path_id);
            assert_eq!(*line, function_line);
        } else {
            assert!(false, "expected a auto-registered step event before the last call one");
        }
        assert!(matches!(should_be_call, TraceLowLevelEvent::Call(CallRecord { .. })));

        let int_value_1 = ValueRecord::Int {
            i: 1,
            type_id: tracer.ensure_type_id(TypeKind::Int, "Int"),
        };
        let int_value_2 = ValueRecord::Int {
            i: 2,
            type_id: tracer.ensure_type_id(TypeKind::Int, "Int"),
        };
        let int_value_3 = ValueRecord::Int {
            i: 3,
            type_id: tracer.ensure_type_id(TypeKind::Int, "Int"),
        };

        tracer.register_variable_with_full_value("test_variable", int_value_1.clone());

        let not_supported_value = ValueRecord::Error {
            msg: "not supported".to_string(),
            type_id: NONE_TYPE_ID,
        };
        tracer.register_variable_with_full_value("test_variable2", not_supported_value);

        tracer.register_cell_value(Place(0), int_value_1.clone());
        let type_id = tracer.ensure_type_id(TypeKind::Seq, "Vector<Int>");
        tracer.register_compound_value(
            Place(1),
            ValueRecord::Sequence {
                elements: vec![ValueRecord::Cell { place: Place(0) }], // #0
                is_slice: false,
                type_id,
            },
        );
        tracer.register_variable("test_variable3", Place(1));
        tracer.assign_cell(Place(1), int_value_2.clone());
        tracer.register_cell_value(Place(2), int_value_2.clone());
        tracer.assign_compound_item(Place(0), 0, Place(2));

        tracer.register_return(NONE_VALUE);
        tracer.drop_variable("test_variable3");

        // example of the history events
        tracer.bind_variable("variable1", Place(1));
        tracer.bind_variable("variable2", Place(2));
        tracer.bind_variable("variable3", Place(3));

        tracer.register_variable_with_full_value("variable1", int_value_1.clone());
        tracer.register_variable_with_full_value("variable2", int_value_2.clone());
        tracer.register_variable_with_full_value("variable3", int_value_3.clone());

        // tracer.assign_simple("variable1", "variable2", PassBy::Value);
        // tracer.assign_compound("variable1", &["variable2", "variable3"], PassBy::Value);

        // more future-proof hopefully, if we add other kinds of RValue
        let rvalue_1 = tracer.simple_rvalue("variable2");
        tracer.assign("variable1", rvalue_1, PassBy::Value);
        let rvalue_2 = tracer.compound_rvalue(&["variable2".to_string(), "variable3".to_string()]);
        tracer.assign("variable1", rvalue_2, PassBy::Value);

        // example for reference types
        let reference_type = TypeRecord {
            kind: TypeKind::Pointer,
            lang_type: "MyReference<Int>".to_string(),
            specific_info: TypeSpecificInfo::Pointer {
                dereference_type_id: tracer.ensure_type_id(TypeKind::Int, "Int"),
            },
        };
        let reference_type_id = tracer.ensure_raw_type_id(reference_type);
        let _reference_value = ValueRecord::Reference {
            dereferenced: Box::new(int_value_1.clone()),
            address: 0,
            mutable: false,
            type_id: reference_type_id,
        };

        tracer.drop_variables(&["variable1".to_string(), "variable2".to_string(), "variable3".to_string()]);

        assert_eq!(tracer.events.len(), 47);
        // visible with
        // cargo tets -- --nocapture
        // println!("{:#?}", tracer.events);

        // tracer.store_trace_metadata(&PathBuf::from("trace_metadata.json")).unwrap();
        // tracer.store_trace_paths(&PathBuf::from("trace_paths.json")).unwrap();
        // tracer.store_trace_events(&PathBuf::from("trace.json")).unwrap();
    }
}
