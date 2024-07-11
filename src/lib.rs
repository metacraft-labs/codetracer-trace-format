mod tracer;
mod types;
pub use crate::tracer::{Tracer, NONE_TYPE_ID, NONE_VALUE};
pub use crate::types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_simple_trace() {
        let mut tracer = Tracer::new("path.small", &vec![]);
        let path = Path::new("/test/path.small");
        tracer.start(path, Line(1));
        tracer.register_step(path, Line(1));
        tracer.register_step(path, Line(2));
        tracer.register_special_event(EventLogKind::Write, "test");
        tracer.register_special_event(EventLogKind::Write, "test2");
        let function_id = tracer.ensure_function_id("function", &path, Line(3));

        let args = vec![tracer.arg("a", NONE_VALUE), tracer.arg("b", NONE_VALUE)];
        tracer.register_call(function_id, args);

        let int_value = ValueRecord::Int {
            i: 1,
            type_id: tracer.ensure_type_id(TypeKind::Int, "Int"),
        };
        tracer.register_variable_with_full_value("test_variable", int_value);
        tracer.register_return(NONE_VALUE);
        assert_eq!(tracer.events.len(), 16);
        // visible with
        // cargo tets -- --nocapture
        // println!("{:#?}", tracer.events);

        // tracer.store_trace_metadata(&PathBuf::from("trace_metadata.json")).unwrap();
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
