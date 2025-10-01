fn main() {
    #[cfg(feature = "writers_and_readers")]
    ::capnpc::CompilerCommand::new().file("src/trace.capnp").run().expect("compiling schema")
}
