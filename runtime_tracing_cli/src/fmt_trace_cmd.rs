use clap::Args;
use serde_json::Value;
use trace_formatter::{
    prettify::{correct_path, prettify_value},
    read_write_json::{save_to_file, serialize_file},
};

#[derive(Debug, Clone, Args)]
pub(crate) struct FmtTraceCommand {
    /// Trace file which we want to format
    source_file: String,

    /// Path where the formatted trace will be saved
    target_file: String,
}

pub(crate) fn run(args: FmtTraceCommand) {
    let ser_json: Value = serialize_file(args.source_file);

    let prettified_json: String = prettify_value(ser_json, "", false);
    let final_pretty_json: String = correct_path(&prettified_json);

    save_to_file(args.target_file, final_pretty_json);
}
