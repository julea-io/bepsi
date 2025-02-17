use super::*;

/// Simple worst-fit bitmap allocator
pub struct WorstFitScan {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
}

impl Allocator for WorstFitScan {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `WorstFitSimple` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        WorstFitScan {
            data: BitArray::new(bitmap),
        }
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied.
    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        let mut worst_fit_offset = None;
        // Initialize with size, as we want the largest possible size and it has to be at least
        // size large.
        let mut worst_fit_size = size;
        let mut offset: u32 = 0;

        while offset + size <= SEGMENT_SIZE as u32 {
            let end_idx = (offset + size) as usize;

            match self.data[offset as usize..end_idx].last_one() {
                Some(last_alloc_idx) => {
                    // Skip to the end of the last allocated block
                    offset += last_alloc_idx as u32 + 1;
                }
                None => {
                    // Find the next allocated block after the current free range
                    match self.data[end_idx..].first_one() {
                        Some(next_alloc_idx) => {
                            let free_block_size = next_alloc_idx as u32 + end_idx as u32 - offset;

                            // Check if this free block is a worse fit (larger)
                            if free_block_size > worst_fit_size {
                                worst_fit_offset = Some(offset);
                                worst_fit_size = free_block_size;
                            }

                            offset = next_alloc_idx as u32 + end_idx as u32 + 1;
                        }
                        None => {
                            // No more allocated blocks, we have scanned the whole segment.
                            let free_block_size = self.data[offset as usize..].len() as u32;

                            // Check if this free block is a worse fit (larger)
                            if free_block_size > worst_fit_size {
                                worst_fit_offset = Some(offset);
                                worst_fit_size = free_block_size;
                            }

                            break;
                        }
                    }
                }
            }
        }

        if let Some(offset) = worst_fit_offset {
            self.mark(offset, size, Action::Allocate);
        }

        worst_fit_offset
    }

    /// Allocates a block of the given `size` at `offset`.
    /// Returns `false` if the allocation request cannot be satisfied.
    fn allocate_at(&mut self, size: u32, offset: u32) -> bool {
        if size == 0 {
            return true;
        }
        if offset + size > SEGMENT_SIZE as u32 {
            return false;
        }

        let start_idx = offset as usize;
        let end_idx = (offset + size) as usize;
        if self.data[start_idx..end_idx].any() {
            return false;
        }
        self.mark(offset, size, Action::Allocate);
        true
    }
}
