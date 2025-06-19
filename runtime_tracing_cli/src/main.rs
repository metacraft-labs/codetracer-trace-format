use std::path::Path;

use clap::{Args, Parser, Subcommand};
use runtime_tracing::{TraceEventsFileFormat, Tracer};

#[derive(Debug, Clone, Args)]
struct ConvertCommand {
    input_file: String,
    output_file: String,
}

#[non_exhaustive]
#[derive(Subcommand, Clone, Debug)]
enum RuntimeTracingCliCommand {
    /// Convert from one trace file format to another
    Convert(ConvertCommand),
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct RuntimeTracingCli {
    #[command(subcommand)]
    command: RuntimeTracingCliCommand,
}

fn determine_file_format_from_name(s: &str) -> Option<TraceEventsFileFormat> {
    if s.ends_with(".json") {
        Some(TraceEventsFileFormat::Json)
    } else if s.ends_with(".bin") {
        Some(TraceEventsFileFormat::Binary)
    } else {
        None
    }
}

fn main() {
    let args = RuntimeTracingCli::parse();

    match args.command {
        RuntimeTracingCliCommand::Convert(convert_command) => {
            let input_file_format = determine_file_format_from_name(&convert_command.input_file).unwrap();
            let output_file_format = determine_file_format_from_name(&convert_command.output_file).unwrap();
            let mut trace = Tracer::new("", &[]);
            trace.load_trace_events(Path::new(&convert_command.input_file), input_file_format).unwrap();
            trace.store_trace_events(Path::new(&convert_command.output_file), output_file_format).unwrap();
        }
    }
}
