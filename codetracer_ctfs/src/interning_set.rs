//! The four interning tables are one unit: a trace carries `paths`, `funcs`,
//! `types` and `varnames` — each as a `.dat` and a `.off` member — all
//! together, or none of them (`internal-files.md` §"Interning Tables").
//!
//! A container holding a strict subset is not a trace with some empty tables:
//! a reader that found `paths.dat` and took an absent `funcs.dat` as empty
//! would answer for a trace that another reader refuses. So the writer
//! refuses to finish such a container and every reader refuses to open one,
//! all with the message built here. The Nim implementation
//! (`codetracer_ctfs/interning_set.nim` in codetracer-trace-format-nim)
//! states it in the same words; `codetracer_trace_reader`'s
//! `interning_table_set_cross_reader` test compares the two readers on the
//! same files.
//!
//! A table that is empty is still present: its `.off` member holds the single
//! offset 0, and the member counts as carried.

/// The eight members, in the order a refusal names them.
pub const INTERNING_TABLE_MEMBERS: [&str; 8] = [
    "paths.dat",
    "paths.off",
    "funcs.dat",
    "funcs.off",
    "types.dat",
    "types.off",
    "varnames.dat",
    "varnames.off",
];

/// The refusal of a container that carries some but not all of
/// [`INTERNING_TABLE_MEMBERS`]: `None` when `has` answers yes for all eight or
/// for none, otherwise the message naming the members carried and the members
/// missing.
pub fn interning_set_refusal(has: impl Fn(&str) -> bool) -> Option<String> {
    let (have, lack): (Vec<&str>, Vec<&str>) = INTERNING_TABLE_MEMBERS.iter().partition(|name| has(name));
    if have.is_empty() || lack.is_empty() {
        return None;
    }
    Some(format!(
        "the container carries {} but not {}: the interning tables paths, funcs, types and varnames are carried \
         all four or none (internal-files.md \"Interning Tables\")",
        have.join(", "),
        lack.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_what_is_carried_and_what_is_missing() {
        assert_eq!(
            interning_set_refusal(|n| n.starts_with("paths.")).as_deref(),
            Some(
                "the container carries paths.dat, paths.off but not funcs.dat, funcs.off, types.dat, types.off, \
                 varnames.dat, varnames.off: the interning tables paths, funcs, types and varnames are carried all \
                 four or none (internal-files.md \"Interning Tables\")"
            )
        );
        assert_eq!(interning_set_refusal(|_| true), None);
        assert_eq!(interning_set_refusal(|_| false), None);
        assert_eq!(interning_set_refusal(|n| n == "markers.dat"), None);
    }
}
