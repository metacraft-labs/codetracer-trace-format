//! The §5d block-number bound, in one place.
//!
//! `CTFS-Binary-Format.md` §5d: a reader MUST accept a container whose length
//! is not a whole number of blocks — the state a crash *inside* an append's
//! tail write leaves — and ignore the bytes past the last whole block. What
//! makes accepting that safe is **flooring**: `whole_blocks` is
//! `floor(length / block_size)`, never `div_ceil`, so the incomplete final
//! block is unaddressable.
//!
//! Flooring only helps if the bound is applied on *every* path from a block
//! number to bytes, and there are three: the entry's mapping root, each mapping
//! block walked to resolve a data block, and the **data block itself**. The
//! last is the easy one to miss, because the final data block's copy is clamped
//! to the entry's `Size` — so a short read out of the partial region
//! *succeeds*. Bounding byte offsets against the file length is therefore not
//! this bound, and a reader that has only that turns a truncated container into
//! wrong content instead of an error.
//!
//! This module exists so the two readers in the crate cannot apply the bound in
//! two of the three places, or word its refusal differently. §5d records
//! exactly that outcome next door: the db-backend's strict path bounds the data
//! block but not the mapping blocks, "the same bound applied in two of three
//! places".

use std::fs::File;

use crate::CtfsError;

/// The container's whole-block count and the numbers a refusal has to quote.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockBound {
    whole_blocks: u64,
    file_len: u64,
    block_size: u32,
}

impl BlockBound {
    /// Derive the bound from the file **as it is now**.
    ///
    /// Deliberately re-derived per read rather than cached at open: a
    /// `ConcurrentCtfsReader` may be following a live producer, and a bound
    /// captured at open would refuse blocks the writer has since materialised.
    pub(crate) fn of(file: &File, block_size: u32) -> Result<Self, CtfsError> {
        let file_len = file.metadata()?.len();
        Ok(BlockBound {
            // floor, never `+ block_size - 1`: rounding up would make the
            // incomplete final block addressable, which is the one arithmetic
            // §5d forbids.
            whole_blocks: file_len / block_size as u64,
            file_len,
            block_size,
        })
    }

    /// Refuse a block number at or past the container's whole blocks, **before**
    /// any of its bytes are touched.
    ///
    /// `what` describes the role the block plays ("mapping root block of
    /// internal file meta.dat", "data block 3 of internal file z.dat", …) so a
    /// consumer learns which stream it lost and why, which is what the Nim and
    /// Go readers report for the same file.
    pub(crate) fn check(&self, block: u64, what: &str) -> Result<(), CtfsError> {
        if block >= self.whole_blocks {
            return Err(self.out_of_bounds(block, what));
        }
        Ok(())
    }

    /// The same refusal for block 0, which no stream may name: it is the root
    /// directory, and the writer uses `0` as the "unallocated" sentinel.
    pub(crate) fn check_mapping_root(&self, block: u64, what: &str) -> Result<(), CtfsError> {
        if block == 0 {
            return Err(self.out_of_bounds(block, what));
        }
        self.check(block, what)
    }

    fn out_of_bounds(&self, block: u64, what: &str) -> CtfsError {
        CtfsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{what} names block {block}, which is out of bounds — the container carries {} whole {}-byte blocks \
                 in {} bytes, so it is truncated or its tail write was interrupted",
                self.whole_blocks, self.block_size, self.file_len
            ),
        ))
    }
}
