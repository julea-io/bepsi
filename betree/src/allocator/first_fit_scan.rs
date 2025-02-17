use super::*;

/// Simple first-fit bitmap allocator
pub struct FirstFitScan {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
}

impl Allocator for FirstFitScan {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `FirstFit` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        FirstFitScan {
            data: BitArray::new(bitmap),
        }
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied and the offset if if can.
    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        let mut offset = 0;

        while offset + size <= SEGMENT_SIZE as u32 {
            let start_idx = offset as usize;
            let end_idx = (offset + size) as usize;

            match self.data[start_idx..end_idx].last_one() {
                Some(last_alloc_idx) => {
                    // Skip to the end of the last allocated block if there is any one at all.
                    offset += last_alloc_idx as u32 + 1
                }
                None => {
                    // No allocated blocks found, so allocate here.
                    self.mark(offset, size, Action::Allocate);
                    return Some(offset);
                }
            }
        }

        None
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
