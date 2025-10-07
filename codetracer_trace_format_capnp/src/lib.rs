mod capnptrace;

pub mod trace_capnp {
    include!(concat!(env!("OUT_DIR"), "/src/trace_capnp.rs"));
}
