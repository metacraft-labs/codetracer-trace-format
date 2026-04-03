use std::sync::atomic::{AtomicU64, Ordering};

/// Simple sequential block allocator.
/// Block 0 is reserved (root block), so allocation starts at 1.
#[derive(Debug)]
pub struct BlockAllocator {
    next_block: u64,
}

impl BlockAllocator {
    pub fn new() -> Self {
        BlockAllocator { next_block: 1 }
    }

    /// Allocate the next block and return its number.
    pub fn alloc(&mut self) -> u64 {
        let block = self.next_block;
        self.next_block += 1;
        block
    }

    /// Return the current next-block value (total blocks allocated + 1).
    pub fn next(&self) -> u64 {
        self.next_block
    }
}

/// Thread-safe atomic block allocator for concurrent writers.
/// Block 0 is reserved (root block), so allocation starts at 1.
#[derive(Debug)]
pub struct AtomicBlockAllocator {
    next_block: AtomicU64,
}

impl AtomicBlockAllocator {
    pub fn new(start: u64) -> Self {
        AtomicBlockAllocator {
            next_block: AtomicU64::new(start),
        }
    }

    /// Atomically allocate the next block and return its number.
    pub fn allocate(&self) -> u64 {
        self.next_block.fetch_add(1, Ordering::Relaxed)
    }

    /// Return the current next-block value (for finalization).
    pub fn next(&self) -> u64 {
        self.next_block.load(Ordering::Relaxed)
    }
}
