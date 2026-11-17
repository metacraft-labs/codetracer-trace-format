//! XXH64, the 64-bit xxHash, as the container's namespace keys use it.
//!
//! `corrmark.ns` keys a correlation by `XXH64(seed = 0, key_bytes)` and
//! confirms a boundary key by a fingerprint under seed 2654435761
//! (`internal-files.md` §"Correlation Index (`corrmark.ns`)"). The function is
//! the reference algorithm, dependency-free so a reader-only build carries no
//! hashing crate.

const PRIME1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME5: u64 = 0x27D4_EB2F_1656_67C5;

fn read_u64(data: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(data[at..at + 8].try_into().expect("eight bytes"))
}

fn read_u32(data: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(data[at..at + 4].try_into().expect("four bytes"))
}

fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(PRIME2)).rotate_left(31).wrapping_mul(PRIME1)
}

fn merge_round(acc: u64, val: u64) -> u64 {
    (acc ^ round(0, val)).wrapping_mul(PRIME1).wrapping_add(PRIME4)
}

/// XXH64 of `data` under `seed`.
pub fn xxh64(data: &[u8], seed: u64) -> u64 {
    let len = data.len();
    let mut idx = 0;
    let mut h = if len >= 32 {
        let mut v1 = seed.wrapping_add(PRIME1).wrapping_add(PRIME2);
        let mut v2 = seed.wrapping_add(PRIME2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(PRIME1);
        while idx + 32 <= len {
            v1 = round(v1, read_u64(data, idx));
            v2 = round(v2, read_u64(data, idx + 8));
            v3 = round(v3, read_u64(data, idx + 16));
            v4 = round(v4, read_u64(data, idx + 24));
            idx += 32;
        }
        let mut h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = merge_round(h, v1);
        h = merge_round(h, v2);
        h = merge_round(h, v3);
        merge_round(h, v4)
    } else {
        seed.wrapping_add(PRIME5)
    };
    h = h.wrapping_add(len as u64);
    while idx + 8 <= len {
        h ^= round(0, read_u64(data, idx));
        h = h.rotate_left(27).wrapping_mul(PRIME1).wrapping_add(PRIME4);
        idx += 8;
    }
    if idx + 4 <= len {
        h ^= u64::from(read_u32(data, idx)).wrapping_mul(PRIME1);
        h = h.rotate_left(23).wrapping_mul(PRIME2).wrapping_add(PRIME3);
        idx += 4;
    }
    while idx < len {
        h ^= u64::from(data[idx]).wrapping_mul(PRIME5);
        h = h.rotate_left(11).wrapping_mul(PRIME1);
        idx += 1;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(PRIME2);
    h ^= h >> 29;
    h = h.wrapping_mul(PRIME3);
    h ^ (h >> 32)
}

#[cfg(test)]
mod tests {
    use super::xxh64;

    const SEED: u64 = 2_654_435_761;

    #[test]
    fn reference_vectors() {
        assert_eq!(xxh64(b"", 0), 0xEF46_DB37_51D8_E999);
        assert_eq!(xxh64(b"", SEED), 0xAC75_FDA2_929B_17EF);
        let corpus = b"Nobody inspects the spammish repetition";
        assert_eq!(xxh64(corpus, 0), 0xFBCE_A83C_8A37_8BF1);
        assert_eq!(xxh64(corpus, SEED), 0x56DB_22DD_5B05_1147);
    }

    /// Every tail length: the 32-byte stripes, the 8-, 4- and 1-byte tails.
    #[test]
    fn length_boundaries() {
        let expected = [
            (1, 0x2078_E1AD_38AD_738B),
            (4, 0x6BB9_9866_CB63_C0A8),
            (7, 0x3136_5618_AD87_4893),
            (8, 0x3BA0_0067_9FBE_E7B5),
            (15, 0xA366_6D45_2D79_E70D),
            (31, 0x7231_3803_63BB_4388),
            (32, 0x5669_9A69_DA28_FD3B),
            (33, 0xD477_4475_9312_4012),
            (63, 0x8898_CD82_19F4_57FC),
            (64, 0xBAD3_3106_0E4C_D79A),
            (70, 0xF508_82C5_53DF_B471),
        ];
        for (n, want) in expected {
            let buf: Vec<u8> = (0..n).map(|i| ((i * 7 + 13) & 0xFF) as u8).collect();
            assert_eq!(xxh64(&buf, 0), want, "length {n}");
        }
    }
}
