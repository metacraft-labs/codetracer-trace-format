//! Members that are not part of the trace format and make a container
//! unreadable.
//!
//! `events.log` (a single combined event stream) and `events.fmt` (its
//! encoding marker) are not trace-format members: a container's events live in
//! its split streams. A container that carries either is refused, by name,
//! before any stream is read, so it is never read as if the member were not
//! there.

use codetracer_ctfs::CtfsReader;

/// The member names a reader refuses.
pub const RETIRED_MEMBERS: [&str; 2] = ["events.log", "events.fmt"];

/// `Err` naming the first retired member `reader`'s container carries.
pub fn refuse_retired_members(reader: &CtfsReader) -> Result<(), String> {
    for name in RETIRED_MEMBERS {
        if reader.file_size(name).is_some() {
            return Err(format!(
                "this container carries `{name}`, which is not part of the trace format; it is refused"
            ));
        }
    }
    Ok(())
}
