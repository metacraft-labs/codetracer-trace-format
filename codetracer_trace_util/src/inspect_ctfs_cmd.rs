use std::fs;
use std::path::Path;

use clap::Args;
use codetracer_ctfs::CtfsReader;

#[derive(Debug, Clone, Args)]
pub(crate) struct InspectCtfsCommand {
    /// Path to the .ct CTFS container file
    input_file: String,

    /// Show detailed block allocation information
    #[arg(long, default_value_t = false)]
    blocks: bool,

    /// Show event statistics
    #[arg(long, default_value_t = false)]
    events: bool,
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

pub(crate) fn run(cmd: InspectCtfsCommand) {
    let path = Path::new(&cmd.input_file);
    let file_size = fs::metadata(path)
        .unwrap_or_else(|e| {
            eprintln!("Error: cannot read file '{}': {}", cmd.input_file, e);
            std::process::exit(1);
        })
        .len();

    let mut reader = CtfsReader::open(path).unwrap_or_else(|e| {
        eprintln!("Error: cannot open CTFS container '{}': {}", cmd.input_file, e);
        std::process::exit(1);
    });

    let block_size = reader.block_size() as u64;
    let max_entries = reader.max_entries();
    let files = reader.list_files();

    println!("CTFS Container: {}", cmd.input_file);
    println!("  File size:      {} bytes", file_size);
    println!("  Block size:     {} bytes", block_size);
    println!("  Version:        2");
    println!("  Max entries:    {}", max_entries);
    println!("  Files:          {}", files.len());
    println!();

    let mut total_data = 0u64;
    let mut total_data_blocks = 0u64;
    let mut total_mapping_blocks = 0u64;

    println!(
        "  {:20} {:>10} {:>8} {:>10} {:>15}",
        "Name", "Size", "Blocks", "Allocated", "Waste"
    );
    println!("  {}", "\u{2500}".repeat(70));

    for name in &files {
        let size = reader.file_size(name).unwrap_or(0);
        let data_blocks = if size == 0 {
            0
        } else {
            (size + block_size - 1) / block_size
        };

        // Each file has at least one mapping block (the root mapping block).
        // For multi-level mappings there could be more, but for a simple
        // estimate we count 1 mapping block per file plus additional ones
        // for files that need more than (block_size/8 - 1) data blocks.
        let n = block_size / 8;
        let usable = n - 1;
        let mapping_blocks = if size == 0 {
            0
        } else if data_blocks <= usable {
            1
        } else {
            // Level 2+: rough estimate of mapping overhead
            let extra = (data_blocks - usable + usable - 1) / usable;
            1 + extra
        };

        let allocated = (data_blocks + mapping_blocks) * block_size;
        let waste = allocated.saturating_sub(size);
        let waste_pct = if allocated > 0 {
            waste as f64 / allocated as f64 * 100.0
        } else {
            0.0
        };

        println!(
            "  {:20} {:>10} {:>8} {:>10} {:>10} ({:.1}%)",
            name,
            format_size(size),
            data_blocks,
            format_size(allocated),
            format_size(waste),
            waste_pct
        );

        total_data += size;
        total_data_blocks += data_blocks;
        total_mapping_blocks += mapping_blocks;

        if cmd.blocks {
            println!(
                "  {:20} data blocks: {}, mapping blocks: {}",
                "", data_blocks, mapping_blocks
            );
        }
    }

    // The root block (block 0) holds the header + file entries
    let root_blocks = 1u64;
    let total_blocks = root_blocks + total_mapping_blocks + total_data_blocks;
    let total_allocated = total_blocks * block_size;
    let overhead = total_allocated.saturating_sub(total_data);
    let overhead_pct = if total_allocated > 0 {
        overhead as f64 / total_allocated as f64 * 100.0
    } else {
        0.0
    };

    println!();
    println!("  Summary:");
    println!("    Data bytes:     {}", format_size(total_data));
    println!(
        "    Allocated:      {} ({} data blocks + {} mapping blocks + {} root block)",
        format_size(total_allocated),
        total_data_blocks,
        total_mapping_blocks,
        root_blocks
    );
    println!(
        "    Overhead:       {} ({:.1}%)",
        format_size(overhead),
        overhead_pct
    );

    if total_data < 1_048_576 {
        println!();
        println!("  Note: Overhead is high for small traces due to {}KB block alignment.", block_size / 1024);
        println!("  For traces > 1MB, overhead is typically < 2%.");
    }

    if cmd.events {
        println!();
        println!("  Events:");
        match reader.read_file("events.log") {
            Ok(data) => {
                println!("    events.log size: {} bytes", data.len());
            }
            Err(e) => {
                println!("    (no events.log found: {})", e);
            }
        }
    }
}
