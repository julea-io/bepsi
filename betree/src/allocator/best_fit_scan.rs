use super::*;

/// Simple best-fit bitmap allocator
pub struct BestFitScan {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
}

impl Allocator for BestFitScan {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `BestFitSimple` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        BestFitScan {
            data: BitArray::new(bitmap),
        }
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied.
    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        let mut best_fit_offset = None;
        // Initialize with a value larger than any possible size
        let mut best_fit_size = SEGMENT_SIZE as u32 + 1;
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

                            // Check if this free block is a better fit
                            if free_block_size >= size && free_block_size < best_fit_size {
                                best_fit_offset = Some(offset);
                                best_fit_size = free_block_size;
                                // If this free block is optimal finish iterating
                                if free_block_size == size {
                                    break;
                                }
                            }

                            offset = next_alloc_idx as u32 + end_idx as u32 + 1;
                        }
                        None => {
                            // No more allocated blocks, we have scanned the whole segment.
                            let free_block_size = self.data[offset as usize..].len() as u32;

                            // Check if this free block is a better fit
                            if free_block_size >= size && free_block_size < best_fit_size {
                                best_fit_offset = Some(offset);
                                best_fit_size = free_block_size;
                            }

                            break;
                        }
                    }
                }
            }
        }

        if let Some(offset) = best_fit_offset {
            self.mark(offset, size, Action::Allocate);
        }

        best_fit_offset
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
