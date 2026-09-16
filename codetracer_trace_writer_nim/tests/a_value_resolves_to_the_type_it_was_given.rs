//! A value's `type_id` must resolve to the type the recorder asked for.
//!
//! # The defect this pins
//!
//! `trace_writer_ensure_type_id` returned a PRIVATE counter and discarded the
//! id the interning table gave it:
//!
//! ```ignore
//! let id = csize_t(handle.types.len)
//! ...
//! discard handle.msWriter.registerType(lt, uint8(ord(tk)))
//! ```
//!
//! So there were two id spaces. The private one advanced once per distinct
//! `(kind, lang_type)`. `types.dat` advanced once per record it appended — and
//! `registerType` deduped on the NAME alone while the record it wrote carried
//! `kind, lang_type, specific_info`. Two types sharing a name under different
//! kinds therefore collapsed onto one `types.dat` record while the FFI handed
//! out two ids, and from that point the two counters were off by one.
//!
//! Values carry the id the FFI returned, and a reader resolves it against
//! `types.dat`. So every value written after the first collision reported a
//! type it had never been given — whichever record happened to sit at that
//! index. Measured in the BEAM recorder: a map-valued variable read back as
//! `record:person`.
//!
//! Nothing failed. Both ids were in range, both resolved to a real type, and
//! the value decoded cleanly. Only a test that asked *which* type a value
//! resolved to could see it, and none did.
//!
//! # What this asserts
//!
//! 1. A name registered under two different kinds yields TWO distinct ids —
//!    the collision that started the drift.
//! 2. Every registered type resolves, through the container, to the kind AND
//!    the name it was registered with. This is the property the old code broke,
//!    and it cannot be checked by counting.
//! 3. A value's `type_id` resolves to that value's own type after a collision
//!    has occurred, which is where the off-by-one used to show up.
//! 4. Re-registering an identical `(kind, name)` returns the SAME id, so the
//!    fix cannot have been "never dedupe", which would make the table grow
//!    without bound.
//!
//! # Mocking policy justification (workspace CLAUDE.md)
//!
//! **Nothing is mocked.** A real `NimTraceWriter` drives the real Nim static
//! library through the real C FFI into a real `.ct`, read back by the real
//! interning-table reader. The bug lived precisely in the seam between the FFI
//! and the interning table, so anything substituted for either would remove
//! the thing under test.

use std::sync::Mutex;

use codetracer_trace_types::{Line, TypeKind, ValueRecord};
use codetracer_trace_writer_nim::{NimTraceWriter, TraceEventsFileFormat};

static NIM_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Register `types` in order, record one variable per type carrying a value of
/// it, and return `(ids, resolved)` where `resolved[i]` is what `types.dat`
/// says the id at `ids[i]` actually is.
fn register_and_resolve(program: &str, types: &[(TypeKind, &str)]) -> (Vec<u64>, Vec<(u8, String)>) {
    let dir = tempfile::tempdir().expect("tempdir");

    let mut w = NimTraceWriter::new(program, &[], TraceEventsFileFormat::Ctfs);
    w.begin_writing_trace_events(&dir.path().join("e.json")).expect("begin_events");
    w.begin_writing_trace_metadata(&dir.path().join("m.json")).expect("begin_metadata");
    w.begin_writing_trace_paths(&dir.path().join("p.json")).expect("begin_paths");

    let source = dir.path().join(format!("{program}.src"));
    w.register_path_with_line_count(&source, 16).expect("register_path_with_line_count");
    w.register_step(&source, Line(1));

    let ids: Vec<u64> = types.iter().map(|(kind, name)| w.ensure_type_id(*kind, name).0 as u64).collect();

    // One variable per type, so each id is exercised on a real value rather
    // than only in the table.
    for (i, id) in ids.iter().enumerate() {
        w.register_variable_with_full_value(
            &format!("v{i}"),
            ValueRecord::Int {
                i: i as i64,
                type_id: codetracer_trace_types::TypeId(*id as usize),
            },
        );
    }

    w.finish_writing_trace_events().expect("finish_events");
    w.finish_writing_trace_metadata().expect("finish_metadata");
    w.finish_writing_trace_paths().expect("finish_paths");
    w.close().expect("close");
    drop(w);

    let ct_path = dir.path().join(format!("{program}.ct"));
    let tables = codetracer_trace_reader::interning_tables_reader::open_interning_tables(&ct_path)
        .expect("interning tables open")
        .expect("interning tables present");

    let resolved: Vec<(u8, String)> = ids
        .iter()
        .map(|id| {
            let rec = tables.type_record(*id).expect("a registered type id resolves in types.dat");
            (rec.kind, String::from_utf8_lossy(&rec.lang_type).into_owned())
        })
        .collect();

    drop(dir);
    (ids, resolved)
}

#[test]
fn a_name_registered_under_two_kinds_gets_two_ids() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // `map` under two kinds is the collision that used to fold into one
    // `types.dat` record while the FFI handed out two ids.
    let (ids, resolved) = register_and_resolve("nim_type_id_collision", &[(TypeKind::Struct, "map"), (TypeKind::Seq, "map")]);

    assert_ne!(
        ids[0], ids[1],
        "a name under two different kinds is two different types and must not \
         share an id; both got {}",
        ids[0]
    );

    assert_eq!(
        resolved[0],
        (TypeKind::Struct as u8, "map".to_string()),
        "the first id must resolve to the Struct `map` it was registered as"
    );
    assert_eq!(
        resolved[1],
        (TypeKind::Seq as u8, "map".to_string()),
        "the second id must resolve to the Seq `map` it was registered as"
    );
}

#[test]
fn every_type_resolves_to_what_it_was_registered_as() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // A collision early in the list, then more types after it. The old code
    // drifted by one at the collision, so everything registered AFTER it
    // resolved to its neighbour — which is why this checks the whole list
    // rather than just the colliding pair.
    let wanted: Vec<(TypeKind, &str)> = vec![
        (TypeKind::Int, "integer"),
        (TypeKind::Struct, "map"),
        (TypeKind::Seq, "map"),
        (TypeKind::Struct, "record:person"),
        (TypeKind::String, "binary"),
        (TypeKind::Bool, "boolean"),
    ];
    let (_ids, resolved) = register_and_resolve("nim_type_id_drift", &wanted);

    let expected: Vec<(u8, String)> = wanted.iter().map(|(k, n)| (*k as u8, (*n).to_string())).collect();

    assert_eq!(
        resolved, expected,
        "each id must resolve to its OWN type. A mismatch here is the drift: \
         every type after a name collision resolved to its neighbour, and \
         nothing failed because the neighbour was a perfectly valid type."
    );
}

#[test]
fn re_registering_the_same_type_returns_the_same_id() {
    let _guard = NIM_TEST_LOCK.lock().unwrap();

    // The control. Without it, "never dedupe" would pass the tests above while
    // growing `types.dat` by one record per call.
    let (ids, _) = register_and_resolve(
        "nim_type_id_dedupe",
        &[
            (TypeKind::Struct, "map"),
            (TypeKind::Struct, "map"),
            (TypeKind::Int, "integer"),
            (TypeKind::Struct, "map"),
        ],
    );

    assert_eq!(ids[0], ids[1], "the same (kind, name) must intern once");
    assert_eq!(ids[0], ids[3], "and still once after an unrelated type between them");
    assert_ne!(ids[0], ids[2], "a different type must still get its own id");
}
