//! Whole-file comparison of two full-profile containers, and, when they
//! differ, the account of where: which blocks, and which member owns each.

use std::fmt::Write as _;

/// The owner of every block of a full-profile container: `"root"` for the
/// root region, `name` for a member's data block, `name#map` for one of its
/// mapping blocks, `"-"` for a block nothing references.
pub fn block_owners(bytes: &[u8]) -> Vec<String> {
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    let bs = u32_at(8) as usize;
    let max = u32_at(12) as usize;
    let header = if bytes[5] >= 6 { 24 } else { 16 };
    let blocks = bytes.len().div_ceil(bs);
    let mut owner = vec!["-".to_string(); blocks];
    let root_blocks = (header + max * 24).div_ceil(bs);
    for o in owner.iter_mut().take(root_blocks) {
        *o = "root".into();
    }
    let n = (bs / 8) as u64;
    for e in 0..max {
        let at = header + e * 24;
        let (size, map, name) = (u64_at(at), u64_at(at + 8), u64_at(at + 16));
        if name == 0 || map == 0 {
            continue;
        }
        let name = codetracer_ctfs::base40_decode(name);
        if map >> 63 == 1 {
            owner[(map & !(1 << 63)) as usize] = name;
            continue;
        }
        let data = size.div_ceil(bs as u64);
        // Level k of the chain holds (n-1)^k data blocks below slots 0..n-2
        // of its block; slot n-1 chains to level k+1.
        #[allow(clippy::too_many_arguments)]
        fn descend(bytes: &[u8], bs: usize, n: u64, block: u64, level: u32, left: &mut u64, owner: &mut [String], name: &str) {
            owner[block as usize] = format!("{name}#map");
            for slot in 0..n - 1 {
                if *left == 0 {
                    return;
                }
                let at = block as usize * bs + slot as usize * 8;
                let p = u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
                if level == 1 {
                    owner[p as usize] = name.to_string();
                    *left -= 1;
                } else {
                    descend(bytes, bs, n, p, level - 1, left, owner, name);
                }
            }
        }
        let mut left = data;
        let mut level_block = map;
        let mut level = 1;
        loop {
            descend(bytes, bs, n, level_block, level, &mut left, &mut owner, &name);
            if left == 0 {
                break;
            }
            let at = level_block as usize * bs + (n as usize - 1) * 8;
            level_block = u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
            level += 1;
        }
    }
    owner
}

/// `Ok` when `a` and `b` are the same bytes; otherwise the blocks that differ
/// with each side's owner, and the order in which each side's blocks were
/// claimed, as runs of one owner.
pub fn compare(a_label: &str, a: &[u8], b_label: &str, b: &[u8]) -> Result<(), String> {
    if a == b {
        return Ok(());
    }
    let bs = u32::from_le_bytes(a[8..12].try_into().unwrap()) as usize;
    let (oa, ob) = (block_owners(a), block_owners(b));
    let mut out = format!(
        "{a_label}: {} bytes, {} blocks; {b_label}: {} bytes, {} blocks\n",
        a.len(),
        oa.len(),
        b.len(),
        ob.len()
    );
    let mut shown = 0;
    for i in 0..oa.len().max(ob.len()) {
        let block = |x: &[u8]| x.get(i * bs..((i + 1) * bs).min(x.len())).map(<[u8]>::to_vec);
        if block(a) != block(b) {
            if shown < 40 {
                let _ = writeln!(
                    out,
                    "  block {i:>5}: {a_label} {:<16} {b_label} {}",
                    oa.get(i).map_or("(none)", String::as_str),
                    ob.get(i).map_or("(none)", String::as_str)
                );
            }
            shown += 1;
        }
    }
    let _ = writeln!(out, "  {shown} blocks differ");
    let runs = |o: &[String]| {
        let mut r: Vec<(String, usize)> = Vec::new();
        for x in o {
            match r.last_mut() {
                Some((n, c)) if n == x => *c += 1,
                _ => r.push((x.clone(), 1)),
            }
        }
        r.iter()
            .map(|(n, c)| if *c > 1 { format!("{n}x{c}") } else { n.clone() })
            .collect::<Vec<_>>()
            .join(" ")
    };
    let _ = writeln!(out, "  {a_label} allocation: {}", runs(&oa));
    let _ = writeln!(out, "  {b_label} allocation: {}", runs(&ob));
    Err(out)
}

/// Panic, with [`compare`]'s account, unless the container files at `nim`
/// and `rust` are the same bytes.
#[allow(dead_code)]
pub fn assert_same_file(nim: &std::path::Path, rust: &std::path::Path, what: &str) {
    let read = |p: &std::path::Path| std::fs::read(p).unwrap_or_else(|e| panic!("{what}: reading {}: {e}", p.display()));
    if let Err(report) = compare("nim", &read(nim), "rust", &read(rust)) {
        panic!("{what}: the two container files differ\n{report}");
    }
}
