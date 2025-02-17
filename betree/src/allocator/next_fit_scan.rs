use super::*;

/// Simple next-fit bitmap allocator
pub struct NextFitScan {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    last_offset: u32,
}

impl Allocator for NextFitScan {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `NextFit` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        NextFitScan {
            data: BitArray::new(bitmap),
            last_offset: 0,
        }
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied.
    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }
        let mut offset = self.last_offset;
        let mut wrap_around = false;

        loop {
            if offset + size > SEGMENT_SIZE as u32 {
                // Wrap around to the beginning of the segment.
                // NOTE: We **can't** set offset here.
                wrap_around = true;
            }

            if wrap_around && offset >= self.last_offset {
                // We've circled back to the starting point, no space found
                return None;
            }

            if offset + size > SEGMENT_SIZE as u32 {
                // NOTE: We **can't** set offset above because of the case if self.last_offset is
                // larger than SEGMENT_SIZE - size. If it is and we would set offset above we would
                // run into an infinite loop. Because the `offset >= self.last_offset` condition
                // would never be fulfilled because offset is reset beforehand.
                offset = 0;
            }

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
                    self.last_offset = offset + size + 1;
                    return Some(offset);
                }
            }
        }
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
        self.last_offset = offset + size;
        true
    }
}
