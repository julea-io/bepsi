use super::*;

/// Simple first-fit that is used for reference. For a more efficient version see FirstFit.
pub struct SegmentAllocator {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
}

impl Allocator for SegmentAllocator {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `FirstFit` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        SegmentAllocator {
            data: BitArray::new(bitmap),
        }
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied and the offset if if can.
    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }
        let offset = {
            let mut idx = 0;
            loop {
                loop {
                    if idx + size > SEGMENT_SIZE as u32 {
                        return None;
                    }
                    if !self.data[idx as usize] {
                        break;
                    }
                    idx += 1;
                }

                let start_idx = (idx + 1) as usize;
                let end_idx = (idx + size) as usize;
                if let Some(first_alloc_idx) = self.data[start_idx..end_idx].first_one() {
                    idx = (idx + 1) + first_alloc_idx as u32 + 1;
                } else {
                    break idx;
                }
            }
        };
        self.mark(offset, size, Action::Allocate);
        return Some(offset);
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
