//! Split binary encoding: compact binary for event envelopes, CBOR only for dynamic payloads.
//!
//! Each event is encoded as:
//! - 1-byte tag (0..23 for each `TraceLowLevelEvent` variant)
//! - Fixed fields in little-endian (u64, i64, u32)
//! - Strings: 4-byte LE length + UTF-8 bytes
//! - Dynamic payloads (ValueRecord, TypeRecord, etc.): 4-byte LE CBOR length + CBOR bytes

use codetracer_trace_types::*;
use std::io::{self, Cursor, Read};

// --- Binary encoding helpers ---

fn write_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}
fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn write_str(out: &mut Vec<u8>, s: &str) {
    write_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}
fn write_cbor<T: serde::Serialize>(out: &mut Vec<u8>, value: &T) {
    let cbor = cbor4ii::serde::to_vec(Vec::new(), value).expect("CBOR encode failed");
    write_u32(out, cbor.len() as u32);
    out.extend_from_slice(&cbor);
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}
fn read_u32(cursor: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}
fn read_u64(cursor: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
fn read_i64(cursor: &mut Cursor<&[u8]>) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}
fn read_str(cursor: &mut Cursor<&[u8]>) -> io::Result<String> {
    let len = read_u32(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
fn read_cbor<T: serde::de::DeserializeOwned>(cursor: &mut Cursor<&[u8]>) -> io::Result<T> {
    let len = read_u32(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    cbor4ii::serde::from_slice(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Encode a single `TraceLowLevelEvent` using split binary+CBOR encoding.
pub fn encode_event(event: &TraceLowLevelEvent, out: &mut Vec<u8>) -> io::Result<()> {
    match event {
        TraceLowLevelEvent::Step(s) => {
            write_u8(out, 0);
            write_u64(out, s.path_id.0 as u64);
            write_i64(out, s.line.0);
        }
        TraceLowLevelEvent::Path(p) => {
            write_u8(out, 1);
            let s = p.to_string_lossy();
            write_str(out, &s);
        }
        TraceLowLevelEvent::VariableName(s) => {
            write_u8(out, 2);
            write_str(out, s);
        }
        TraceLowLevelEvent::Variable(s) => {
            write_u8(out, 3);
            write_str(out, s);
        }
        TraceLowLevelEvent::Type(t) => {
            write_u8(out, 4);
            write_cbor(out, t);
        }
        TraceLowLevelEvent::Value(fvr) => {
            write_u8(out, 5);
            write_u64(out, fvr.variable_id.0 as u64);
            write_cbor(out, &fvr.value);
        }
        TraceLowLevelEvent::Function(f) => {
            write_u8(out, 6);
            write_u64(out, f.path_id.0 as u64);
            write_i64(out, f.line.0);
            write_str(out, &f.name);
        }
        TraceLowLevelEvent::Call(c) => {
            write_u8(out, 7);
            write_u64(out, c.function_id.0 as u64);
            write_cbor(out, &c.args);
        }
        TraceLowLevelEvent::Return(r) => {
            write_u8(out, 8);
            write_cbor(out, &r.return_value);
        }
        TraceLowLevelEvent::Event(e) => {
            write_u8(out, 9);
            write_u8(out, e.kind as u8);
            write_str(out, &e.metadata);
            write_str(out, &e.content);
        }
        TraceLowLevelEvent::Asm(lines) => {
            write_u8(out, 10);
            write_u32(out, lines.len() as u32);
            for line in lines {
                write_str(out, line);
            }
        }
        TraceLowLevelEvent::BindVariable(bv) => {
            write_u8(out, 11);
            write_u64(out, bv.variable_id.0 as u64);
            write_i64(out, bv.place.0);
        }
        TraceLowLevelEvent::Assignment(a) => {
            write_u8(out, 12);
            write_cbor(out, a);
        }
        TraceLowLevelEvent::DropVariables(ids) => {
            write_u8(out, 13);
            write_u32(out, ids.len() as u32);
            for id in ids {
                write_u64(out, id.0 as u64);
            }
        }
        TraceLowLevelEvent::CompoundValue(cv) => {
            write_u8(out, 14);
            write_i64(out, cv.place.0);
            write_cbor(out, &cv.value);
        }
        TraceLowLevelEvent::CellValue(cv) => {
            write_u8(out, 15);
            write_i64(out, cv.place.0);
            write_cbor(out, &cv.value);
        }
        TraceLowLevelEvent::AssignCompoundItem(a) => {
            write_u8(out, 16);
            write_i64(out, a.place.0);
            write_u64(out, a.index as u64);
            write_i64(out, a.item_place.0);
        }
        TraceLowLevelEvent::AssignCell(a) => {
            write_u8(out, 17);
            write_i64(out, a.place.0);
            write_cbor(out, &a.new_value);
        }
        TraceLowLevelEvent::VariableCell(vc) => {
            write_u8(out, 18);
            write_u64(out, vc.variable_id.0 as u64);
            write_i64(out, vc.place.0);
        }
        TraceLowLevelEvent::DropVariable(id) => {
            write_u8(out, 19);
            write_u64(out, id.0 as u64);
        }
        TraceLowLevelEvent::ThreadStart(id) => {
            write_u8(out, 20);
            write_u64(out, id.0);
        }
        TraceLowLevelEvent::ThreadExit(id) => {
            write_u8(out, 21);
            write_u64(out, id.0);
        }
        TraceLowLevelEvent::ThreadSwitch(id) => {
            write_u8(out, 22);
            write_u64(out, id.0);
        }
        TraceLowLevelEvent::DropLastStep => {
            write_u8(out, 23);
        }
    }
    Ok(())
}

/// Decode a single `TraceLowLevelEvent` from split binary+CBOR encoding.
pub fn decode_event(cursor: &mut Cursor<&[u8]>) -> io::Result<TraceLowLevelEvent> {
    let tag = read_u8(cursor)?;
    match tag {
        0 => {
            let path_id = PathId(read_u64(cursor)? as usize);
            let line = Line(read_i64(cursor)?);
            Ok(TraceLowLevelEvent::Step(StepRecord { path_id, line }))
        }
        1 => {
            let s = read_str(cursor)?;
            Ok(TraceLowLevelEvent::Path(s.into()))
        }
        2 => Ok(TraceLowLevelEvent::VariableName(read_str(cursor)?)),
        3 => Ok(TraceLowLevelEvent::Variable(read_str(cursor)?)),
        4 => Ok(TraceLowLevelEvent::Type(read_cbor(cursor)?)),
        5 => {
            let variable_id = VariableId(read_u64(cursor)? as usize);
            let value: ValueRecord = read_cbor(cursor)?;
            Ok(TraceLowLevelEvent::Value(FullValueRecord {
                variable_id,
                value,
            }))
        }
        6 => {
            let path_id = PathId(read_u64(cursor)? as usize);
            let line = Line(read_i64(cursor)?);
            let name = read_str(cursor)?;
            Ok(TraceLowLevelEvent::Function(FunctionRecord {
                path_id,
                line,
                name,
            }))
        }
        7 => {
            let function_id = FunctionId(read_u64(cursor)? as usize);
            let args: Vec<FullValueRecord> = read_cbor(cursor)?;
            Ok(TraceLowLevelEvent::Call(CallRecord {
                function_id,
                args,
            }))
        }
        8 => {
            let return_value: ValueRecord = read_cbor(cursor)?;
            Ok(TraceLowLevelEvent::Return(ReturnRecord { return_value }))
        }
        9 => {
            let kind_byte = read_u8(cursor)?;
            let kind: EventLogKind =
                num_traits::FromPrimitive::from_u8(kind_byte).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown EventLogKind: {}", kind_byte),
                    )
                })?;
            let metadata = read_str(cursor)?;
            let content = read_str(cursor)?;
            Ok(TraceLowLevelEvent::Event(RecordEvent {
                kind,
                metadata,
                content,
            }))
        }
        10 => {
            let count = read_u32(cursor)? as usize;
            let mut lines = Vec::with_capacity(count);
            for _ in 0..count {
                lines.push(read_str(cursor)?);
            }
            Ok(TraceLowLevelEvent::Asm(lines))
        }
        11 => {
            let variable_id = VariableId(read_u64(cursor)? as usize);
            let place = Place(read_i64(cursor)?);
            Ok(TraceLowLevelEvent::BindVariable(BindVariableRecord {
                variable_id,
                place,
            }))
        }
        12 => Ok(TraceLowLevelEvent::Assignment(read_cbor(cursor)?)),
        13 => {
            let count = read_u32(cursor)? as usize;
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(VariableId(read_u64(cursor)? as usize));
            }
            Ok(TraceLowLevelEvent::DropVariables(ids))
        }
        14 => {
            let place = Place(read_i64(cursor)?);
            let value: ValueRecord = read_cbor(cursor)?;
            Ok(TraceLowLevelEvent::CompoundValue(CompoundValueRecord {
                place,
                value,
            }))
        }
        15 => {
            let place = Place(read_i64(cursor)?);
            let value: ValueRecord = read_cbor(cursor)?;
            Ok(TraceLowLevelEvent::CellValue(CellValueRecord {
                place,
                value,
            }))
        }
        16 => {
            let place = Place(read_i64(cursor)?);
            let index = read_u64(cursor)? as usize;
            let item_place = Place(read_i64(cursor)?);
            Ok(TraceLowLevelEvent::AssignCompoundItem(
                AssignCompoundItemRecord {
                    place,
                    index,
                    item_place,
                },
            ))
        }
        17 => {
            let place = Place(read_i64(cursor)?);
            let new_value: ValueRecord = read_cbor(cursor)?;
            Ok(TraceLowLevelEvent::AssignCell(AssignCellRecord {
                place,
                new_value,
            }))
        }
        18 => {
            let variable_id = VariableId(read_u64(cursor)? as usize);
            let place = Place(read_i64(cursor)?);
            Ok(TraceLowLevelEvent::VariableCell(VariableCellRecord {
                variable_id,
                place,
            }))
        }
        19 => Ok(TraceLowLevelEvent::DropVariable(VariableId(
            read_u64(cursor)? as usize,
        ))),
        20 => Ok(TraceLowLevelEvent::ThreadStart(ThreadId(read_u64(
            cursor,
        )?))),
        21 => Ok(TraceLowLevelEvent::ThreadExit(ThreadId(read_u64(cursor)?))),
        22 => Ok(TraceLowLevelEvent::ThreadSwitch(ThreadId(read_u64(
            cursor,
        )?))),
        23 => Ok(TraceLowLevelEvent::DropLastStep),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown event tag: {}", tag),
        )),
    }
}

/// Encode multiple events, returning the concatenated bytes and per-event sizes.
pub fn encode_events(events: &[TraceLowLevelEvent]) -> (Vec<u8>, Vec<usize>) {
    let mut buf = Vec::new();
    let mut sizes = Vec::new();
    for event in events {
        let start = buf.len();
        encode_event(event, &mut buf).expect("encode failed");
        sizes.push(buf.len() - start);
    }
    (buf, sizes)
}

/// Decode all events from a byte buffer.
pub fn decode_events(data: &[u8]) -> Vec<TraceLowLevelEvent> {
    let mut events = Vec::new();
    let mut cursor = Cursor::new(data);
    while (cursor.position() as usize) < data.len() {
        match decode_event(&mut cursor) {
            Ok(event) => events.push(event),
            Err(_) => break,
        }
    }
    events
}

/// Build a lazy event offset index for a decompressed chunk.
/// Returns byte offsets of each event within the data.
pub fn scan_event_offsets(data: &[u8]) -> Vec<u32> {
    let mut offsets = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        offsets.push(pos as u32);
        pos += event_byte_size(data, pos);
    }
    offsets
}

/// Compute the byte size of the event at `offset` in `data`.
///
/// This must match the encoding in `encode_event` exactly.
fn event_byte_size(data: &[u8], offset: usize) -> usize {
    let tag = data[offset];
    match tag {
        0 => 17, // Step: tag(1) + path_id(8) + line(8)
        1 | 2 | 3 => {
            // Path, VariableName, Variable: tag(1) + str_len(4) + string
            let str_len = u32::from_le_bytes(
                data[offset + 1..offset + 5].try_into().unwrap(),
            ) as usize;
            5 + str_len
        }
        4 => {
            // Type: tag(1) + cbor_len(4) + cbor
            let cbor_len = u32::from_le_bytes(
                data[offset + 1..offset + 5].try_into().unwrap(),
            ) as usize;
            5 + cbor_len
        }
        5 => {
            // Value: tag(1) + var_id(8) + cbor_len(4) + cbor
            let cbor_len = u32::from_le_bytes(
                data[offset + 9..offset + 13].try_into().unwrap(),
            ) as usize;
            13 + cbor_len
        }
        6 => {
            // Function: tag(1) + path_id(8) + line(8) + name_len(4) + name
            let name_len = u32::from_le_bytes(
                data[offset + 17..offset + 21].try_into().unwrap(),
            ) as usize;
            21 + name_len
        }
        7 => {
            // Call: tag(1) + func_id(8) + cbor_len(4) + cbor
            let cbor_len = u32::from_le_bytes(
                data[offset + 9..offset + 13].try_into().unwrap(),
            ) as usize;
            13 + cbor_len
        }
        8 => {
            // Return: tag(1) + cbor_len(4) + cbor
            let cbor_len = u32::from_le_bytes(
                data[offset + 1..offset + 5].try_into().unwrap(),
            ) as usize;
            5 + cbor_len
        }
        9 => {
            // Event: tag(1) + kind(1) + meta_len(4) + meta + content_len(4) + content
            let meta_len = u32::from_le_bytes(
                data[offset + 2..offset + 6].try_into().unwrap(),
            ) as usize;
            let content_len = u32::from_le_bytes(
                data[offset + 6 + meta_len..offset + 10 + meta_len]
                    .try_into()
                    .unwrap(),
            ) as usize;
            10 + meta_len + content_len
        }
        10 => {
            // Asm: tag(1) + count(4) + [str_len(4) + str]...
            let count = u32::from_le_bytes(
                data[offset + 1..offset + 5].try_into().unwrap(),
            ) as usize;
            let mut pos = offset + 5;
            for _ in 0..count {
                let len = u32::from_le_bytes(
                    data[pos..pos + 4].try_into().unwrap(),
                ) as usize;
                pos += 4 + len;
            }
            pos - offset
        }
        11 => 17, // BindVariable: tag(1) + var_id(8) + place(8)
        12 => {
            // Assignment: tag(1) + cbor_len(4) + cbor
            let cbor_len = u32::from_le_bytes(
                data[offset + 1..offset + 5].try_into().unwrap(),
            ) as usize;
            5 + cbor_len
        }
        13 => {
            // DropVariables: tag(1) + count(4) + [u64]...
            let count = u32::from_le_bytes(
                data[offset + 1..offset + 5].try_into().unwrap(),
            ) as usize;
            5 + count * 8
        }
        14 | 15 => {
            // CompoundValue, CellValue: tag(1) + place(8) + cbor_len(4) + cbor
            let cbor_len = u32::from_le_bytes(
                data[offset + 9..offset + 13].try_into().unwrap(),
            ) as usize;
            13 + cbor_len
        }
        16 => 25, // AssignCompoundItem: tag(1) + place(8) + index(8) + item_place(8)
        17 => {
            // AssignCell: tag(1) + place(8) + cbor_len(4) + cbor
            let cbor_len = u32::from_le_bytes(
                data[offset + 9..offset + 13].try_into().unwrap(),
            ) as usize;
            13 + cbor_len
        }
        18 => 17, // VariableCell: tag(1) + var_id(8) + place(8)
        19 => 9,  // DropVariable: tag(1) + var_id(8)
        20 | 21 | 22 => 9, // ThreadStart/Exit/Switch: tag(1) + thread_id(8)
        23 => 1,  // DropLastStep: tag(1)
        _ => panic!("unknown split-binary event tag: {}", tag),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_binary_step_roundtrip() {
        let event = TraceLowLevelEvent::Step(StepRecord {
            path_id: PathId(42),
            line: Line(100),
        });
        let mut buf = Vec::new();
        encode_event(&event, &mut buf).unwrap();
        assert_eq!(buf.len(), 17); // tag(1) + path_id(8) + line(8)

        let mut cursor = Cursor::new(buf.as_slice());
        let decoded = decode_event(&mut cursor).unwrap();
        assert_eq!(format!("{:?}", event), format!("{:?}", decoded));
    }

    #[test]
    fn test_split_binary_string_variants() {
        let events = vec![
            TraceLowLevelEvent::Path("/test/path.rs".into()),
            TraceLowLevelEvent::VariableName("my_var".to_string()),
            TraceLowLevelEvent::Variable("x".to_string()),
        ];

        let (buf, sizes) = encode_events(&events);
        assert_eq!(sizes.len(), 3);

        let decoded = decode_events(&buf);
        assert_eq!(decoded.len(), 3);
        assert_eq!(format!("{:?}", events), format!("{:?}", decoded));
    }

    #[test]
    fn test_split_binary_event_byte_size() {
        let events = vec![
            TraceLowLevelEvent::Step(StepRecord {
                path_id: PathId(1),
                line: Line(10),
            }),
            TraceLowLevelEvent::Path("/hello".into()),
            TraceLowLevelEvent::DropLastStep,
        ];

        let (buf, sizes) = encode_events(&events);

        // Verify event_byte_size matches actual sizes
        let offsets = scan_event_offsets(&buf);
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1] as usize, sizes[0]);
        assert_eq!(offsets[2] as usize, sizes[0] + sizes[1]);
    }

    #[test]
    fn test_split_binary_all_fixed_size_variants() {
        let events = vec![
            TraceLowLevelEvent::Step(StepRecord {
                path_id: PathId(1),
                line: Line(1),
            }),
            TraceLowLevelEvent::BindVariable(BindVariableRecord {
                variable_id: VariableId(5),
                place: Place(10),
            }),
            TraceLowLevelEvent::AssignCompoundItem(AssignCompoundItemRecord {
                place: Place(1),
                index: 2,
                item_place: Place(3),
            }),
            TraceLowLevelEvent::VariableCell(VariableCellRecord {
                variable_id: VariableId(7),
                place: Place(8),
            }),
            TraceLowLevelEvent::DropVariable(VariableId(99)),
            TraceLowLevelEvent::ThreadStart(ThreadId(1)),
            TraceLowLevelEvent::ThreadExit(ThreadId(2)),
            TraceLowLevelEvent::ThreadSwitch(ThreadId(3)),
            TraceLowLevelEvent::DropLastStep,
        ];

        let (buf, _sizes) = encode_events(&events);
        let decoded = decode_events(&buf);
        assert_eq!(decoded.len(), events.len());
        assert_eq!(format!("{:?}", events), format!("{:?}", decoded));
    }
}
