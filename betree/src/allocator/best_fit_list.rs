use super::*;

/// Simple Best-Fit bitmap allocator that uses a list to manage free segments
pub struct BestFitList {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    free_segments: Vec<(u32, u32)>, // (offset, size) of free segments
}

impl Allocator for BestFitList {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `BestFitList` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        let data = BitArray::new(bitmap);
        let mut allocator = BestFitList {
            data,
            free_segments: Vec::new(),
        };
        allocator.initialize_free_segments();
        allocator
    }

    /// Allocates a block of the given `size` using best-fit strategy.
    /// Returns `None` if the allocation request cannot be satisfied.
    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        let mut best_fit_segment_index: Option<usize> = None;
        let mut best_fit_segment_size: u32 = u32::MAX; // Initialize with a large value

        for i in 0..self.free_segments.len() {
            let (_, segment_size) = self.free_segments[i];
            if segment_size >= size && segment_size < best_fit_segment_size {
                best_fit_segment_index = Some(i);
                best_fit_segment_size = segment_size;
            }
        }

        if let Some(index) = best_fit_segment_index {
            let (offset, segment_size) = self.free_segments[index];
            self.mark(offset, size, Action::Allocate);

            self.free_segments[index].0 = offset + size;
            self.free_segments[index].1 = segment_size - size;

            return Some(offset);
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

        // Update free_segments to reflect the allocation - similar to FirstFitList::allocate_at
        for i in 0..self.free_segments.len() {
            let (seg_offset, seg_size) = self.free_segments[i];
            if seg_offset == offset && seg_size == size {
                self.free_segments.remove(i);
                self.mark(offset, size, Action::Allocate);
                return true;
            } else if seg_offset == offset && seg_size > size {
                self.free_segments[i].0 += size;
                self.free_segments[i].1 -= size;
                self.mark(offset, size, Action::Allocate);
                return true;
            } else if offset > seg_offset && offset + size == seg_offset + seg_size {
                self.free_segments[i].1 -= size;
                self.mark(offset, size, Action::Allocate);
                return true;
            } else if offset > seg_offset
                && offset < seg_offset + seg_size
                && offset + size < seg_offset + seg_size
            {
                let remaining_size = seg_size - (size + (offset - seg_offset));
                let new_offset = offset + size;
                self.free_segments[i].1 = offset - seg_offset;

                self.free_segments
                    .insert(i + 1, (new_offset, remaining_size));
                self.mark(offset, size, Action::Allocate);
                return true;
            }
        }

        false
    }
}

impl BestFitList {
    /// Initializes the `free_segments` vector by scanning the bitmap.
    fn initialize_free_segments(&mut self) {
        let mut offset: u32 = 0;
        while offset < SEGMENT_SIZE as u32 {
            if !self.data()[offset as usize] {
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
        // keep segments sorted by offset
        self.free_segments.sort_by_key(|seg| seg.0);
    }
}
