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

/// Mapping blocks a closed container gives a member of `size` bytes
/// (`ctfs-container.md` §2 and §4): none for an empty member or one of at
/// most one block, which is stored as a single direct data block; otherwise
/// the level-1 block, and for each further level its own block plus the
/// lower-level blocks that hold that level's share of the data blocks.
pub(crate) fn mapping_blocks_of(size: u64, block_size: u64) -> u64 {
    if size <= block_size {
        return 0;
    }
    let usable = block_size / 8 - 1;
    let mut remaining = size.div_ceil(block_size).saturating_sub(usable);
    let mut total = 1u64;
    let mut level = 2u32;
    while remaining > 0 && level <= 5 {
        let here = remaining.min(usable.saturating_pow(level));
        total += 1;
        for k in 1..level {
            total += here.div_ceil(usable.saturating_pow(k));
        }
        remaining -= here;
        level += 1;
    }
    total
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
    println!("  Version:        {}", codetracer_ctfs::header::VERSION);
    println!("  Max entries:    {}", max_entries);
    println!("  Files:          {}", files.len());
    println!();

    let mut total_data = 0u64;
    let mut total_data_blocks = 0u64;
    let mut total_mapping_blocks = 0u64;

    println!("  {:20} {:>10} {:>8} {:>10} {:>15}", "Name", "Size", "Blocks", "Allocated", "Waste");
    println!("  {}", "\u{2500}".repeat(70));

    for name in &files {
        let size = reader.file_size(name).unwrap_or(0);
        let data_blocks = if size == 0 { 0 } else { size.div_ceil(block_size) };

        let mapping_blocks = mapping_blocks_of(size, block_size);

        let allocated = (data_blocks + mapping_blocks) * block_size;
        let waste = allocated.saturating_sub(size);
        let waste_pct = if allocated > 0 { waste as f64 / allocated as f64 * 100.0 } else { 0.0 };

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
            println!("  {:20} data blocks: {}, mapping blocks: {}", "", data_blocks, mapping_blocks);
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
    println!("    Overhead:       {} ({:.1}%)", format_size(overhead), overhead_pct);

    if total_data < 1_048_576 {
        println!();
        println!(
            "  Note: Overhead is high for small traces due to {}KB block alignment.",
            block_size / 1024
        );
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

#[cfg(test)]
mod tests {
    use super::mapping_blocks_of;

    /// `ctfs-container.md` §2 and §4: an empty member and a member of at most
    /// one block own no mapping block; past that, one level-1 block, then one
    /// block per level plus the lower-level blocks beneath it.
    #[test]
    fn mapping_blocks_follow_the_member_layout() {
        let bs = 4096u64;
        let usable = bs / 8 - 1;
        assert_eq!(mapping_blocks_of(0, bs), 0, "an empty member");
        assert_eq!(mapping_blocks_of(1, bs), 0, "a one-byte member is direct");
        assert_eq!(mapping_blocks_of(bs, bs), 0, "a full one-block member is direct");
        assert_eq!(mapping_blocks_of(bs + 1, bs), 1, "two data blocks: one level-1 block");
        assert_eq!(mapping_blocks_of(usable * bs, bs), 1, "a full level 1");
        // One more data block: a level-2 block and the level-1 block below it.
        assert_eq!(mapping_blocks_of(usable * bs + 1, bs), 3);
        // Level 1 full plus 2 * usable + 1 data blocks at level 2: the level-2
        // block and three level-1 blocks below it.
        assert_eq!(mapping_blocks_of((usable + 2 * usable + 1) * bs, bs), 1 + 1 + 3);
    }
}
