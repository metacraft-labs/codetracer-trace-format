//! `ct_value_*`: a streaming encoder that writes a value as the CBOR map its
//! `ValueRecord` serializes to, field by field, without building the record.
//!
//! A compound value is opened with a `ct_value_begin_*` call, its elements are
//! written, and `ct_value_end_compound` writes the fields that follow the
//! elements in the record's encoding (`type_id`, and `is_slice`, `address`,
//! `mutable` where the kind has them). Element counts are written as given;
//! nothing checks that as many elements follow.

use crate::{bytes, guarded, set_error};

/// Compounds nest at most this deep.
pub const MAX_NESTING_DEPTH: usize = 32;

#[derive(Clone, Copy)]
enum Frame {
    Struct { type_id: u64 },
    Sequence { type_id: u64, is_slice: bool },
    Tuple { type_id: u64 },
    Variant { type_id: u64 },
    Reference { type_id: u64, address: u64, mutable: bool },
}

/// The encoder behind a `value_encoder_t`.
#[derive(Default)]
pub struct ValueEncoder {
    buf: Vec<u8>,
    stack: Vec<Frame>,
}

fn head(buf: &mut Vec<u8>, major: u8, value: u64) {
    let mt = major << 5;
    if value <= 23 {
        buf.push(mt | value as u8);
    } else if value <= 0xff {
        buf.extend_from_slice(&[mt | 24, value as u8]);
    } else if value <= 0xffff {
        buf.push(mt | 25);
        buf.extend_from_slice(&(value as u16).to_be_bytes());
    } else if value <= 0xffff_ffff {
        buf.push(mt | 26);
        buf.extend_from_slice(&(value as u32).to_be_bytes());
    } else {
        buf.push(mt | 27);
        buf.extend_from_slice(&value.to_be_bytes());
    }
}

impl ValueEncoder {
    fn uint(&mut self, v: u64) {
        head(&mut self.buf, 0, v);
    }

    fn int(&mut self, v: i64) {
        if v >= 0 {
            head(&mut self.buf, 0, v as u64);
        } else {
            head(&mut self.buf, 1, (-1 - v) as u64);
        }
    }

    fn text(&mut self, s: &[u8]) {
        head(&mut self.buf, 3, s.len() as u64);
        self.buf.extend_from_slice(s);
    }

    fn byte_string(&mut self, s: &[u8]) {
        head(&mut self.buf, 2, s.len() as u64);
        self.buf.extend_from_slice(s);
    }

    fn boolean(&mut self, b: bool) {
        self.buf.push(if b { 0xf5 } else { 0xf4 });
    }

    fn map(&mut self, n: u64) {
        head(&mut self.buf, 5, n);
    }

    fn array(&mut self, n: u64) {
        head(&mut self.buf, 4, n);
    }

    fn kind(&mut self, entries: u64, kind: &str) {
        self.map(entries);
        self.text(b"kind");
        self.text(kind.as_bytes());
    }

    fn type_id(&mut self, id: u64) {
        self.text(b"type_id");
        self.uint(id);
    }

    pub fn write_int(&mut self, value: i64, type_id: u64) {
        self.kind(3, "Int");
        self.text(b"i");
        self.int(value);
        self.type_id(type_id);
    }

    pub fn write_float(&mut self, value: f64, type_id: u64) {
        self.kind(3, "Float");
        self.text(b"f");
        self.buf.push(0xfb);
        self.buf.extend_from_slice(&value.to_bits().to_be_bytes());
        self.type_id(type_id);
    }

    pub fn write_bool(&mut self, value: bool, type_id: u64) {
        self.kind(4, "Bool");
        self.text(b"b");
        self.boolean(value);
        self.text(b"text");
        self.text(if value { b"true" } else { b"false" });
        self.type_id(type_id);
    }

    pub fn write_string(&mut self, text: &[u8], type_id: u64) {
        self.kind(3, "String");
        self.text(b"text");
        self.text(text);
        self.type_id(type_id);
    }

    pub fn write_none(&mut self, type_id: u64) {
        self.kind(2, "None");
        self.type_id(type_id);
    }

    pub fn write_raw(&mut self, raw: &[u8], type_id: u64) {
        self.kind(3, "Raw");
        self.text(b"r");
        self.text(raw);
        self.type_id(type_id);
    }

    pub fn write_error(&mut self, msg: &[u8], type_id: u64) {
        self.kind(3, "Error");
        self.text(b"msg");
        self.text(msg);
        self.type_id(type_id);
    }

    /// A character, as a one-byte text string of `byte`.
    pub fn write_char(&mut self, byte: u8, type_id: u64) {
        self.kind(3, "Char");
        self.text(b"c");
        self.text(&[byte]);
        self.type_id(type_id);
    }

    pub fn write_bigint(&mut self, magnitude: &[u8], negative: bool, type_id: u64) {
        self.kind(4, "BigInt");
        self.text(b"b");
        self.byte_string(magnitude);
        self.text(b"negative");
        self.boolean(negative);
        self.type_id(type_id);
    }

    fn push(&mut self, frame: Frame) -> Result<(), String> {
        if self.stack.len() >= MAX_NESTING_DEPTH {
            return Err(format!("nesting too deep (max {MAX_NESTING_DEPTH})"));
        }
        self.stack.push(frame);
        Ok(())
    }

    pub fn begin_struct(&mut self, type_id: u64, field_count: u64) -> Result<(), String> {
        if self.stack.len() >= MAX_NESTING_DEPTH {
            return Err(format!("nesting too deep (max {MAX_NESTING_DEPTH})"));
        }
        self.kind(3, "Struct");
        self.text(b"field_values");
        self.array(field_count);
        self.push(Frame::Struct { type_id })
    }

    pub fn begin_sequence(&mut self, type_id: u64, element_count: u64, is_slice: bool) -> Result<(), String> {
        if self.stack.len() >= MAX_NESTING_DEPTH {
            return Err(format!("nesting too deep (max {MAX_NESTING_DEPTH})"));
        }
        self.kind(4, "Sequence");
        self.text(b"elements");
        self.array(element_count);
        self.push(Frame::Sequence { type_id, is_slice })
    }

    pub fn begin_tuple(&mut self, type_id: u64, element_count: u64) -> Result<(), String> {
        if self.stack.len() >= MAX_NESTING_DEPTH {
            return Err(format!("nesting too deep (max {MAX_NESTING_DEPTH})"));
        }
        self.kind(3, "Tuple");
        self.text(b"elements");
        self.array(element_count);
        self.push(Frame::Tuple { type_id })
    }

    pub fn begin_variant(&mut self, discriminator: &[u8], type_id: u64) -> Result<(), String> {
        if self.stack.len() >= MAX_NESTING_DEPTH {
            return Err(format!("nesting too deep (max {MAX_NESTING_DEPTH})"));
        }
        self.kind(4, "Variant");
        self.text(b"discriminator");
        self.text(discriminator);
        self.text(b"contents");
        self.push(Frame::Variant { type_id })
    }

    pub fn begin_reference(&mut self, address: u64, mutable: bool, type_id: u64) -> Result<(), String> {
        if self.stack.len() >= MAX_NESTING_DEPTH {
            return Err(format!("nesting too deep (max {MAX_NESTING_DEPTH})"));
        }
        self.kind(5, "Reference");
        self.text(b"dereferenced");
        self.push(Frame::Reference { type_id, address, mutable })
    }

    pub fn end_compound(&mut self) -> Result<(), String> {
        let Some(frame) = self.stack.pop() else {
            return Err("endCompound without matching begin".to_string());
        };
        match frame {
            Frame::Struct { type_id } | Frame::Tuple { type_id } | Frame::Variant { type_id } => self.type_id(type_id),
            Frame::Sequence { type_id, is_slice } => {
                self.text(b"is_slice");
                self.boolean(is_slice);
                self.type_id(type_id);
            }
            Frame::Reference { type_id, address, mutable } => {
                self.text(b"address");
                self.uint(address);
                self.text(b"mutable");
                self.boolean(mutable);
                self.type_id(type_id);
            }
        }
        Ok(())
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.stack.clear();
    }

    pub fn bytes(&self) -> &[u8] {
        &self.buf
    }

    /// The encoded value, leaving the encoder empty.
    pub fn take_bytes(&mut self) -> Vec<u8> {
        self.stack.clear();
        std::mem::take(&mut self.buf)
    }
}

/// The CBOR of `ValueRecord::Int`.
pub(crate) fn int_value(value: i64, type_id: u64) -> Vec<u8> {
    let mut e = ValueEncoder::default();
    e.write_int(value, type_id);
    e.take_bytes()
}

/// The CBOR of `ValueRecord::Raw`.
pub(crate) fn raw_value(raw: &[u8], type_id: u64) -> Vec<u8> {
    let mut e = ValueEncoder::default();
    e.write_raw(raw, type_id);
    e.take_bytes()
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

pub type ValueEncoderHandle = ValueEncoder;

/// Run `f` on a non-NULL encoder, mapping its result to 0 / 1.
fn on_encoder(name: &str, h: *mut ValueEncoder, f: impl FnOnce(&mut ValueEncoder) -> Result<(), String>) -> i32 {
    guarded(name, None, 1, || {
        if h.is_null() {
            set_error("NULL handle");
            return 1;
        }
        // SAFETY: a non-NULL encoder handle is one `ct_value_encoder_new`
        // returned (the handle invariant).
        match f(unsafe { &mut *h }) {
            Ok(()) => 0,
            Err(e) => {
                set_error(&e);
                1
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn ct_value_encoder_new() -> *mut ValueEncoder {
    guarded("ct_value_encoder_new", None, std::ptr::null_mut(), || Box::into_raw(Box::default()))
}

/// # Safety
/// `h` is NULL or an encoder `ct_value_encoder_new` returned, not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_encoder_free(h: *mut ValueEncoder) {
    guarded("ct_value_encoder_free", None, (), || {
        if !h.is_null() {
            drop(unsafe { Box::from_raw(h) });
        }
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_encoder_reset(h: *mut ValueEncoder) {
    guarded("ct_value_encoder_reset", None, (), || {
        if !h.is_null() {
            unsafe { &mut *h }.reset();
        }
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_int(h: *mut ValueEncoder, value: i64, type_id: u64) -> i32 {
    on_encoder("ct_value_write_int", h, |e| {
        e.write_int(value, type_id);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_float(h: *mut ValueEncoder, value: f64, type_id: u64) -> i32 {
    on_encoder("ct_value_write_float", h, |e| {
        e.write_float(value, type_id);
        Ok(())
    })
}

/// A bool with type id 0.
///
/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_bool(h: *mut ValueEncoder, value: i32) -> i32 {
    on_encoder("ct_value_write_bool", h, |e| {
        e.write_bool(value != 0, 0);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_bool_typed(h: *mut ValueEncoder, value: i32, type_id: u64) -> i32 {
    on_encoder("ct_value_write_bool_typed", h, |e| {
        e.write_bool(value != 0, type_id);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder; `(data, len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_string(h: *mut ValueEncoder, data: *const u8, len: usize, type_id: u64) -> i32 {
    on_encoder("ct_value_write_string", h, |e| {
        e.write_string(unsafe { bytes(data, len) }, type_id);
        Ok(())
    })
}

/// A None with type id 0.
///
/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_none(h: *mut ValueEncoder) -> i32 {
    on_encoder("ct_value_write_none", h, |e| {
        e.write_none(0);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_none_typed(h: *mut ValueEncoder, type_id: u64) -> i32 {
    on_encoder("ct_value_write_none_typed", h, |e| {
        e.write_none(type_id);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder; `(data, len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_raw(h: *mut ValueEncoder, data: *const u8, len: usize, type_id: u64) -> i32 {
    on_encoder("ct_value_write_raw", h, |e| {
        e.write_raw(unsafe { bytes(data, len) }, type_id);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder; `(data, len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_error(h: *mut ValueEncoder, data: *const u8, len: usize, type_id: u64) -> i32 {
    on_encoder("ct_value_write_error", h, |e| {
        e.write_error(unsafe { bytes(data, len) }, type_id);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_begin_struct(h: *mut ValueEncoder, type_id: u64, field_count: i32) -> i32 {
    on_encoder("ct_value_begin_struct", h, |e| e.begin_struct(type_id, field_count as i64 as u64))
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_begin_sequence(h: *mut ValueEncoder, type_id: u64, element_count: i32) -> i32 {
    on_encoder("ct_value_begin_sequence", h, |e| {
        e.begin_sequence(type_id, element_count as i64 as u64, false)
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_begin_sequence_with_slice(h: *mut ValueEncoder, type_id: u64, element_count: i32, is_slice: i32) -> i32 {
    on_encoder("ct_value_begin_sequence_with_slice", h, |e| {
        e.begin_sequence(type_id, element_count as i64 as u64, is_slice != 0)
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_begin_tuple(h: *mut ValueEncoder, type_id: u64, element_count: i32) -> i32 {
    on_encoder("ct_value_begin_tuple", h, |e| e.begin_tuple(type_id, element_count as i64 as u64))
}

/// # Safety
/// `h` is NULL or a live encoder; `(discriminator, disc_len)` is readable or
/// NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_begin_variant(h: *mut ValueEncoder, discriminator: *const u8, disc_len: usize, type_id: u64) -> i32 {
    on_encoder("ct_value_begin_variant", h, |e| {
        if disc_len > 0 && discriminator.is_null() {
            return Err("NULL discriminator with non-zero len".to_string());
        }
        e.begin_variant(unsafe { bytes(discriminator, disc_len) }, type_id)
    })
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_begin_reference(h: *mut ValueEncoder, address: u64, mutable: i32, type_id: u64) -> i32 {
    on_encoder("ct_value_begin_reference", h, |e| e.begin_reference(address, mutable != 0, type_id))
}

/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_end_compound(h: *mut ValueEncoder) -> i32 {
    on_encoder("ct_value_end_compound", h, |e| e.end_compound())
}

/// A character: the low byte of `codepoint`, as a one-byte text string.
///
/// # Safety
/// `h` is NULL or a live encoder.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_char(h: *mut ValueEncoder, codepoint: u32, type_id: u64) -> i32 {
    on_encoder("ct_value_write_char", h, |e| {
        e.write_char((codepoint & 0xff) as u8, type_id);
        Ok(())
    })
}

/// # Safety
/// `h` is NULL or a live encoder; `(data, len)` is readable or NULL/0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_write_bigint(h: *mut ValueEncoder, data: *const u8, len: usize, negative: i32, type_id: u64) -> i32 {
    on_encoder("ct_value_write_bigint", h, |e| {
        if len > 0 && data.is_null() {
            return Err("NULL data with non-zero len".to_string());
        }
        e.write_bigint(unsafe { bytes(data, len) }, negative != 0, type_id);
        Ok(())
    })
}

/// The bytes encoded so far, valid until the next call on the encoder; NULL
/// when there are none.
///
/// # Safety
/// `h` is NULL or a live encoder; `out_len` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ct_value_get_bytes(h: *mut ValueEncoder, out_len: *mut usize) -> *const u8 {
    guarded("ct_value_get_bytes", None, std::ptr::null(), || {
        if h.is_null() || out_len.is_null() {
            return std::ptr::null();
        }
        let e = unsafe { &*h };
        unsafe { *out_len = e.buf.len() };
        if e.buf.is_empty() { std::ptr::null() } else { e.buf.as_ptr() }
    })
}
