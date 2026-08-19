//! Write the demo container natively, so the same trace can be produced on a
//! host and inside wasm and the two compared byte for byte.
//!
//!   cargo run -p wasm-ctfs-demo --example dump -- out.ct [steps]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let out = args.next().ok_or("usage: dump <out.ct> [steps]")?;
    let steps: u32 = args.next().unwrap_or_else(|| "64".into()).parse()?;
    let bytes = wasm_ctfs_demo::build_container(steps)?;
    std::fs::write(&out, &bytes)?;
    eprintln!("wrote {out} ({} bytes)", bytes.len());
    Ok(())
}
