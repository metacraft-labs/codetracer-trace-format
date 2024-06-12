use std::path::PathBuf;
mod tracer;
mod types;
pub use crate::tracer::{Tracer, NONE_TYPE_ID, NONE_VALUE};
pub use crate::types::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_trace() {
        let mut tracer = Tracer::new("path.small", &vec![]);
        let path = PathBuf::from("/test/path.small");
        tracer.start(&path, Line(1));
        tracer.register_step(&path, Line(1));
        tracer.register_step(&path, Line(2));
        tracer.register_special_event(EventLogKind::Write, "test");
        tracer.register_special_event(EventLogKind::Write, "test2");
        let function_id = tracer.ensure_function_id("function", &path, Line(3));
        tracer.register_call(function_id, vec![]);

        let int_value = ValueRecord::Int {
            i: 1,
            type_id: tracer.ensure_type_id(TypeKind::Int, "Int")
        };
        tracer.register_variable_with_full_value("test_variable", int_value);
        tracer.register_return(NONE_VALUE);
        assert_eq!(tracer.events.len(), 14);
        // visible with
        // cargo tets -- --nocapture
        println!("{:#?}", tracer.events);
    }
}
