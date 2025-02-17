use super::*;

/// Simple first-fit bitmap allocator that uses a list to manage free segments
pub struct FirstFitList {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    free_segments: Vec<(u32, u32)>, // (offset, size) of free segments
}

impl Allocator for FirstFitList {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `ListFirstFit` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        let data = BitArray::new(bitmap);
        let mut allocator = FirstFitList {
            data,
            free_segments: Vec::new(),
        };
        allocator.initialize_free_segments();
        allocator
    }

    /// Allocates a block of the given `size`.
    /// Returns `None` if the allocation request cannot be satisfied and the offset if if can.
    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        for i in 0..self.free_segments.len() {
            let (offset, segment_size) = self.free_segments[i];

            if segment_size >= size {
                self.mark(offset, size, Action::Allocate);

                // update the free segment with the remaining size and new offset
                self.free_segments[i].0 = offset + size;
                self.free_segments[i].1 = segment_size - size;
                // NOTE: We do not handle the == case here. We could remove that entry from the
                // list but we then would need to copy some things because the allocate_at (and
                // deallocation) logic depends on a sorted list and also need have extra handling.
                // The empty slots get garbage collected on the next sync anyway.
                return Some(offset);
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

        // Update free_segments to reflect the allocation
        for i in 0..self.free_segments.len() {
            let (seg_offset, seg_size) = self.free_segments[i];
            if seg_offset == offset && seg_size == size {
                // perfect fit, remove the segment
                self.free_segments.remove(i);
                self.mark(offset, size, Action::Allocate);
                return true;
            } else if seg_offset == offset && seg_size > size {
                // allocation at the beginning of the segment, adjust offset and size
                self.free_segments[i].0 += size;
                self.free_segments[i].1 -= size;
                self.mark(offset, size, Action::Allocate);
                return true;
            } else if offset > seg_offset && offset + size == seg_offset + seg_size {
                // allocation at the end of the segment, just adjust size
                self.free_segments[i].1 -= size;
                self.mark(offset, size, Action::Allocate);
                return true;
            } else if offset > seg_offset
                && offset < seg_offset + seg_size
                && offset + size < seg_offset + seg_size
            {
                // allocation in the middle of the segment, split segment
                let remaining_size = seg_size - (size + (offset - seg_offset));
                let new_offset = offset + size;
                self.free_segments[i].1 = offset - seg_offset; // adjust current segment size

                self.free_segments
                    .insert(i + 1, (new_offset, remaining_size)); // insert new segment after current
                self.mark(offset, size, Action::Allocate);
                return true;
            }
        }

        false // No suitable free segment found in free_segments list
    }
}

impl FirstFitList {
    /// Initializes the `free_segments` vector by scanning the bitmap.
    fn initialize_free_segments(&mut self) {
        let mut offset: u32 = 0;
        while offset < SEGMENT_SIZE as u32 {
            if !self.data()[offset as usize] {
                // If bit is 0, it's free
                let start_offset = offset;
                let mut current_size = 0;
                while offset < SEGMENT_SIZE as u32 && !self.data()[offset as usize] {
                    current_size += 1;
                    offset += 1;
                }
                self.free_segments.push((start_offset, current_size));
            } else {
                offset += 1;
            }
        }
    }
}
