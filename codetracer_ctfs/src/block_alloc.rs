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
