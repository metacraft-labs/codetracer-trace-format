mod tracer;
mod types;
pub use crate::tracer::{Tracer, NONE_TYPE_ID, NONE_VALUE};
pub use crate::types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    // use std::path::PathBuf;

    #[test]
    fn test_simple_trace() {
        let mut tracer = Tracer::new("path.small", &vec![]);
        let path = Path::new("/test/path.small");
        tracer.start(path, Line(1));
        tracer.register_step(path, Line(1));
        tracer.register_step(path, Line(2));
        tracer.register_special_event(EventLogKind::Write, "test");
        tracer.register_special_event(EventLogKind::Write, "test2");

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

        let int_value = ValueRecord::Int {
            i: 1,
            type_id: tracer.ensure_type_id(TypeKind::Int, "Int"),
        };
        let int_value_2 = ValueRecord::Int {
            i: 2,
            type_id: tracer.ensure_type_id(TypeKind::Int, "Int"),
        };
        tracer.register_variable_with_full_value("test_variable", int_value.clone());

        let not_supported_value = ValueRecord::Error {
            msg: "not supported".to_string(),
            type_id: NONE_TYPE_ID,
        };
        tracer.register_variable_with_full_value("test_variable2", not_supported_value);

        tracer.register_cell_value(ValueId(0), int_value);
        let type_id = tracer.ensure_type_id(TypeKind::Seq, "Vector<Int>");
        tracer.register_compound_value(
            ValueId(1),
            ValueRecord::Sequence {
                elements: vec![ValueRecord::Cell { value_id: ValueId(0) }], // #0
                type_id,
            },
        );
        tracer.register_variable_cell("test_variable3", ValueId(1));
        tracer.assign_cell(ValueId(1), int_value_2.clone());
        tracer.register_cell_value(ValueId(2), int_value_2.clone());
        tracer.assign_compound_item(ValueId(0), 0, ValueId(2));

        tracer.register_return(NONE_VALUE);

        assert_eq!(tracer.events.len(), 31);
        // visible with
        // cargo tets -- --nocapture
        // println!("{:#?}", tracer.events);

        // tracer.store_trace_metadata(&PathBuf::from("trace_metadata.json")).unwrap();
        // tracer.store_trace_paths(&PathBuf::from("trace_paths.json")).unwrap();
        // tracer.store_trace_events(&PathBuf::from("trace.json")).unwrap();
    }

    #[test]
    fn test_equality_of_value_records() {
        let a = ValueRecord::Int { i: 0, type_id: TypeId(0) }; // just an example type_id
        let b = ValueRecord::Int { i: 0, type_id: TypeId(0) };
        let different = ValueRecord::Int { i: 1, type_id: TypeId(0) };

        assert_eq!(a, b);
        assert_ne!(a, different);
    }
}
