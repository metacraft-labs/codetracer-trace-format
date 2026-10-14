//! The `steps.dat` encoding rule (`trace-events.md` §"Encoding Rules").
//!
//! Position records — `AbsoluteStep` (tag 0), `DeltaStep` (1) and
//! `DeltaColumn` (7) — each leave the *cursor* at the position they denote;
//! every other record leaves it where it was. There is one cursor for the
//! whole stream, and it does not survive a chunk boundary. For a position `p`:
//!
//! 1. the first position record of a chunk is an `AbsoluteStep`, whatever
//!    records precede it in the chunk;
//! 2. otherwise, with `d = p - cursor`, it is a delta — `DeltaColumn` for a
//!    registered column step, `DeltaStep` otherwise — when the varint of
//!    `zigzag(d)` is strictly shorter than the varint of `p`;
//! 3. otherwise it is an `AbsoluteStep`: a tie goes to the absolute.
//!
//! Nothing else forces an absolute: not a call, a return, a thread switch or
//! a source reload. Every `steps.dat` this crate writes goes through
//! [`ChunkCursor::encode_position`], so the two step paths cannot disagree.

/// Tag of an `AbsoluteStep` record.
pub const TAG_ABSOLUTE_STEP: u8 = 0;
/// Tag of a `DeltaStep` record.
pub const TAG_DELTA_STEP: u8 = 1;
/// Tag of a `DeltaColumn` record.
pub const TAG_DELTA_COLUMN: u8 = 7;

/// The encoded length of an unsigned LEB128 varint.
pub fn varint_len(v: u64) -> usize {
    if v == 0 { 1 } else { (64 - v.leading_zeros() as usize).div_ceil(7) }
}

/// Zigzag-map a signed value onto the unsigned varint space.
pub fn zigzag(d: i64) -> u64 {
    ((d << 1) ^ (d >> 63)) as u64
}

fn put_varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// The cursor of one chunk: `None` at the chunk's start, the last position
/// record's position afterwards.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkCursor(Option<u64>);

impl ChunkCursor {
    /// A cursor at the start of a chunk.
    pub fn new() -> Self {
        ChunkCursor(None)
    }

    /// Forget the cursor: the next position opens a chunk.
    pub fn reset(&mut self) {
        self.0 = None;
    }

    /// The cursor's position, if a position record has been written in this
    /// chunk.
    pub fn position(&self) -> Option<u64> {
        self.0
    }

    /// Append the record for position `p` to `out`, choosing its form by the
    /// rule, and move the cursor to `p`. `column_step` says the recorder
    /// registered it as a column step, which decides the delta's tag.
    pub fn encode_position(&mut self, p: u64, column_step: bool, out: &mut Vec<u8>) {
        let delta = self.0.and_then(|c| i64::try_from(p as i128 - c as i128).ok());
        match delta {
            Some(d) if varint_len(zigzag(d)) < varint_len(p) => {
                out.push(if column_step { TAG_DELTA_COLUMN } else { TAG_DELTA_STEP });
                put_varint(zigzag(d), out);
            }
            _ => {
                out.push(TAG_ABSOLUTE_STEP);
                put_varint(p, out);
            }
        }
        self.0 = Some(p);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(cursor: Option<u64>, p: u64, column: bool) -> Vec<u8> {
        let mut c = ChunkCursor(cursor);
        let mut out = Vec::new();
        c.encode_position(p, column, &mut out);
        assert_eq!(c.position(), Some(p), "the cursor moves to the position");
        out
    }

    #[test]
    fn varint_lengths_at_the_boundaries() {
        for (v, n) in [(0u64, 1), (127, 1), (128, 2), (16383, 2), (16384, 3), (u64::MAX, 10)] {
            assert_eq!(varint_len(v), n, "{v}");
        }
        assert_eq!(zigzag(-64), 127);
        assert_eq!(zigzag(63), 126);
        assert_eq!(zigzag(64), 128);
    }

    #[test]
    fn the_first_position_of_a_chunk_is_absolute() {
        assert_eq!(enc(None, 5000, false), vec![0, 0x88, 0x27]);
        assert_eq!(enc(None, 5000, true), vec![0, 0x88, 0x27], "a column step too");
    }

    #[test]
    fn a_strictly_shorter_delta_wins_and_a_tie_goes_to_the_absolute() {
        assert_eq!(enc(Some(199), 200, false), vec![1, 2], "1 byte against 2");
        assert_eq!(enc(Some(199), 200, true), vec![7, 2], "a column step's delta is a DeltaColumn");
        assert_eq!(enc(Some(99), 100, false), vec![0, 100], "1 byte each: a tie");
        assert_eq!(enc(Some(0), 200, false), vec![0, 0xc8, 0x01], "2 bytes each: a tie");
        assert_eq!(enc(Some(100_000), 300, false), vec![0, 0xac, 0x02], "3 bytes against 2");
        assert_eq!(enc(Some(300), 100_000, false), vec![0, 0xa0, 0x8d, 0x06], "3 bytes each: a tie");
    }

    #[test]
    fn a_delta_wider_than_i64_is_absolute() {
        assert_eq!(enc(Some(0), u64::MAX, false)[0], 0);
        assert_eq!(enc(Some(u64::MAX), 1, false), vec![0, 1]);
    }
}
