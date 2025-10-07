/// The next 3 bytes are reserved/version info.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADERV1: &[u8] = &[
    0xC0, 0xDE, 0x72, 0xAC, 0xE2, // The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
    0x01, // Indicates version 1 of the file format
    0x00, 0x00,
]; // Reserved, must be zero in this version.
