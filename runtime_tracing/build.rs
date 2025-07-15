fn main() {
    ::capnpc::CompilerCommand::new().file("src/trace.capnp").run().expect("compiling schema")
}
