use std::path::Path;

use crate::fmt_trace_cmd::FmtTraceCommand;
use clap::{Args, Parser, Subcommand};
use runtime_tracing::{create_trace_reader, create_trace_writer, TraceEventsFileFormat};
mod fmt_trace_cmd;

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
    /// Format a trace which is in JSON file format
    FormatTrace(FmtTraceCommand),
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
            let mut trace_reader = create_trace_reader(input_file_format);
            let mut trace_writer = create_trace_writer("", &[], output_file_format);
            let mut trace_events = trace_reader.load_trace_events(Path::new(&convert_command.input_file)).unwrap();
            trace_writer.begin_writing_trace_events(Path::new(&convert_command.output_file)).unwrap();
            trace_writer.append_events(&mut trace_events);
            trace_writer.finish_writing_trace_events().unwrap();
        }
        RuntimeTracingCliCommand::FormatTrace(fmt_trace_cmd) => {
            fmt_trace_cmd::run(fmt_trace_cmd);
        }
    }
}
