/// platform.bin — binary platform description format for CTFS portable traces.
///
/// Describes the OS, architecture, pointer size, endianness, page size,
/// kernel version, and libc version of the machine that recorded the trace.
///
/// Wire format uses LEB128 varints for string lengths and little-endian
/// fixed-width integers, consistent with the rest of the CTFS format.

use crate::filemap::{decode_leb128, encode_leb128};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PlatformOs {
    Linux = 0,
    MacOS = 1,
    Windows = 2,
    FreeBSD = 3,
}

impl PlatformOs {
    fn from_u8(v: u8) -> Result<Self, String> {
        match v {
            0 => Ok(PlatformOs::Linux),
            1 => Ok(PlatformOs::MacOS),
            2 => Ok(PlatformOs::Windows),
            3 => Ok(PlatformOs::FreeBSD),
            _ => Err(format!("unknown platform os: {}", v)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PlatformArch {
    X86_64 = 0,
    Aarch64 = 1,
    RiscV64 = 2,
}

impl PlatformArch {
    fn from_u8(v: u8) -> Result<Self, String> {
        match v {
            0 => Ok(PlatformArch::X86_64),
            1 => Ok(PlatformArch::Aarch64),
            2 => Ok(PlatformArch::RiscV64),
            _ => Err(format!("unknown platform arch: {}", v)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlatformInfo {
    pub os: PlatformOs,
    pub arch: PlatformArch,
    pub pointer_size: u8,
    pub endianness: u8,
    pub page_size: u32,
    pub kernel_major: u16,
    pub kernel_minor: u16,
    pub kernel_patch: u16,
    pub libc_name: String,
    pub kernel_version: String,
}

const PLATFORM_MAGIC: [u8; 4] = [0x50, 0x4C, 0x41, 0x54]; // "PLAT"

/// Fixed header size after magic: os(1) + arch(1) + ptrSize(1) + endian(1) +
/// pageSize(4) + kernelMajor(2) + kernelMinor(2) + kernelPatch(2) + reserved(6) = 20
const PLATFORM_FIXED_SIZE: usize = 20;

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

impl PlatformInfo {
    /// Serialize a PlatformInfo to binary.
    ///
    /// Format: "PLAT" (4) + os(1) + arch(1) + ptrSize(1) + endian(1) +
    ///         pageSize(u32 LE) + kernelMajor(u16 LE) + kernelMinor(u16 LE) +
    ///         kernelPatch(u16 LE) + reserved(6) +
    ///         libcName_len(varint) + libcName + kernelVersion_len(varint) + kernelVersion
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Magic
        buf.extend_from_slice(&PLATFORM_MAGIC);

        // Fixed fields
        buf.push(self.os as u8);
        buf.push(self.arch as u8);
        buf.push(self.pointer_size);
        buf.push(self.endianness);
        buf.extend_from_slice(&self.page_size.to_le_bytes());
        buf.extend_from_slice(&self.kernel_major.to_le_bytes());
        buf.extend_from_slice(&self.kernel_minor.to_le_bytes());
        buf.extend_from_slice(&self.kernel_patch.to_le_bytes());

        // Reserved 6 bytes
        buf.extend_from_slice(&[0u8; 6]);

        // libcName (varint-prefixed string)
        buf.extend_from_slice(&encode_leb128(self.libc_name.len() as u64));
        buf.extend_from_slice(self.libc_name.as_bytes());

        // kernelVersion (varint-prefixed string)
        buf.extend_from_slice(&encode_leb128(self.kernel_version.len() as u64));
        buf.extend_from_slice(self.kernel_version.as_bytes());

        buf
    }

    /// Deserialize a PlatformInfo from binary. Validates magic.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        let min_size = 4 + PLATFORM_FIXED_SIZE;
        if data.len() < min_size {
            return Err("platform data too short for header".to_string());
        }

        // Validate magic
        if data[0..4] != PLATFORM_MAGIC {
            return Err("invalid platform magic".to_string());
        }

        let mut pos = 4;

        // os
        let os = PlatformOs::from_u8(data[pos])?;
        pos += 1;

        // arch
        let arch = PlatformArch::from_u8(data[pos])?;
        pos += 1;

        // pointerSize
        let pointer_size = data[pos];
        pos += 1;

        // endianness
        let endianness = data[pos];
        pos += 1;

        // pageSize
        if pos + 4 > data.len() {
            return Err("truncated platform: expected pageSize".to_string());
        }
        let page_size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        // kernelMajor
        if pos + 2 > data.len() {
            return Err("truncated platform: expected kernelMajor".to_string());
        }
        let kernel_major = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;

        // kernelMinor
        if pos + 2 > data.len() {
            return Err("truncated platform: expected kernelMinor".to_string());
        }
        let kernel_minor = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;

        // kernelPatch
        if pos + 2 > data.len() {
            return Err("truncated platform: expected kernelPatch".to_string());
        }
        let kernel_patch = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;

        // Skip reserved 6 bytes
        if pos + 6 > data.len() {
            return Err("truncated platform: expected reserved bytes".to_string());
        }
        pos += 6;

        // libcName
        if pos >= data.len() {
            return Err("truncated platform: expected libcName_len".to_string());
        }
        let (libc_len, libc_var_bytes) = decode_leb128(data, pos)
            .map_err(|e| format!("truncated platform: invalid libcName_len varint: {}", e))?;
        pos += libc_var_bytes;
        let libc_len = libc_len as usize;
        if pos + libc_len > data.len() {
            return Err("truncated platform: expected libcName bytes".to_string());
        }
        let libc_name = String::from_utf8(data[pos..pos + libc_len].to_vec())
            .map_err(|e| format!("invalid UTF-8 in libcName: {}", e))?;
        pos += libc_len;

        // kernelVersion
        if pos >= data.len() {
            return Err("truncated platform: expected kernelVersion_len".to_string());
        }
        let (kv_len, kv_var_bytes) = decode_leb128(data, pos)
            .map_err(|e| format!("truncated platform: invalid kernelVersion_len varint: {}", e))?;
        pos += kv_var_bytes;
        let kv_len = kv_len as usize;
        if pos + kv_len > data.len() {
            return Err("truncated platform: expected kernelVersion bytes".to_string());
        }
        let kernel_version = String::from_utf8(data[pos..pos + kv_len].to_vec())
            .map_err(|e| format!("invalid UTF-8 in kernelVersion: {}", e))?;

        Ok(PlatformInfo {
            os,
            arch,
            pointer_size,
            endianness,
            page_size,
            kernel_major,
            kernel_minor,
            kernel_patch,
            libc_name,
            kernel_version,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_roundtrip() {
        let p = PlatformInfo {
            os: PlatformOs::Linux,
            arch: PlatformArch::X86_64,
            pointer_size: 8,
            endianness: 0,
            page_size: 4096,
            kernel_major: 6,
            kernel_minor: 12,
            kernel_patch: 63,
            libc_name: "glibc-2.40".to_string(),
            kernel_version: "6.12.63-generic".to_string(),
        };
        let data = p.serialize();

        let p2 = PlatformInfo::deserialize(&data).unwrap();
        assert_eq!(p2.os, PlatformOs::Linux);
        assert_eq!(p2.arch, PlatformArch::X86_64);
        assert_eq!(p2.pointer_size, 8);
        assert_eq!(p2.endianness, 0);
        assert_eq!(p2.page_size, 4096);
        assert_eq!(p2.kernel_major, 6);
        assert_eq!(p2.kernel_minor, 12);
        assert_eq!(p2.kernel_patch, 63);
        assert_eq!(p2.libc_name, "glibc-2.40");
        assert_eq!(p2.kernel_version, "6.12.63-generic");
    }

    #[test]
    fn test_platform_all_variants() {
        // Test all OS variants
        for &os in &[PlatformOs::Linux, PlatformOs::MacOS, PlatformOs::Windows, PlatformOs::FreeBSD] {
            let p = PlatformInfo {
                os,
                arch: PlatformArch::X86_64,
                pointer_size: 8,
                endianness: 0,
                page_size: 4096,
                kernel_major: 5,
                kernel_minor: 10,
                kernel_patch: 0,
                libc_name: "test".to_string(),
                kernel_version: "5.10.0".to_string(),
            };
            let data = p.serialize();
            let p2 = PlatformInfo::deserialize(&data).unwrap();
            assert_eq!(p2.os, os);
        }

        // Test all arch variants
        for &arch in &[PlatformArch::X86_64, PlatformArch::Aarch64, PlatformArch::RiscV64] {
            let p = PlatformInfo {
                os: PlatformOs::Linux,
                arch,
                pointer_size: 8,
                endianness: 0,
                page_size: 4096,
                kernel_major: 5,
                kernel_minor: 10,
                kernel_patch: 0,
                libc_name: "test".to_string(),
                kernel_version: "5.10.0".to_string(),
            };
            let data = p.serialize();
            let p2 = PlatformInfo::deserialize(&data).unwrap();
            assert_eq!(p2.arch, arch);
        }
    }

    #[test]
    fn test_platform_invalid_magic() {
        let p = PlatformInfo {
            os: PlatformOs::Linux,
            arch: PlatformArch::X86_64,
            pointer_size: 8,
            endianness: 0,
            page_size: 4096,
            kernel_major: 5,
            kernel_minor: 10,
            kernel_patch: 0,
            libc_name: "glibc-2.35".to_string(),
            kernel_version: "5.10.0".to_string(),
        };
        let mut data = p.serialize();
        // Corrupt the magic
        data[0] = b'X';
        data[1] = b'Y';
        let res = PlatformInfo::deserialize(&data);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("invalid platform magic"));
    }

    #[test]
    fn test_platform_truncated() {
        // Too short for header
        let res = PlatformInfo::deserialize(&[0x50, 0x4C]);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("too short"));

        // Valid magic but truncated fixed fields
        let res2 = PlatformInfo::deserialize(&[0x50, 0x4C, 0x41, 0x54, 0x00]);
        assert!(res2.is_err());
        assert!(res2.unwrap_err().contains("too short"));
    }

    #[test]
    fn test_platform_empty_strings() {
        let p = PlatformInfo {
            os: PlatformOs::Linux,
            arch: PlatformArch::X86_64,
            pointer_size: 8,
            endianness: 0,
            page_size: 4096,
            kernel_major: 0,
            kernel_minor: 0,
            kernel_patch: 0,
            libc_name: "".to_string(),
            kernel_version: "".to_string(),
        };
        let data = p.serialize();
        let p2 = PlatformInfo::deserialize(&data).unwrap();
        assert_eq!(p2.libc_name, "");
        assert_eq!(p2.kernel_version, "");
    }

    /// Build the canonical compat platform used by both Nim and Rust compat tests.
    pub(crate) fn build_compat_platform() -> PlatformInfo {
        PlatformInfo {
            os: PlatformOs::Linux,
            arch: PlatformArch::X86_64,
            pointer_size: 8,
            endianness: 0,
            page_size: 4096,
            kernel_major: 6,
            kernel_minor: 12,
            kernel_patch: 63,
            libc_name: "glibc-2.40".to_string(),
            kernel_version: "6.12.63-generic".to_string(),
        }
    }
}
