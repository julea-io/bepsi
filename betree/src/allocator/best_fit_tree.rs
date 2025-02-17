use std::collections::BTreeMap;

use super::*;

/// This is a true best fit allocator. It will always find the best fit if available.
pub struct BestFitTree {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    tree: BTreeMap<u32, Vec<u32>>, // store free segments sorted by size (size, offset)
}

impl Allocator for BestFitTree {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `BestFitTree` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        let data = BitArray::new(bitmap);
        let mut allocator = BestFitTree {
            data,
            tree: BTreeMap::new(),
        };
        allocator.build_tree();
        allocator
    }

    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        if let Some((&segment_size, offsets)) = self.tree.range(size..).next() {
            let best_fit_offset = *offsets.first().unwrap();
            self.mark(best_fit_offset, size, Action::Allocate);

            // Update free segments tree
            self.remove_free_segment(segment_size, best_fit_offset);
            if segment_size > size {
                self.insert_free_segment(segment_size - size, best_fit_offset + size);
            }

            return Some(best_fit_offset);
        }

        None
    }

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

        // Rebuild the tree to reflect changes. This is **not** efficient but the easiest solution,
        // as the allocate_at is called only **once** on loading the bitmap from disk.
        self.build_tree();
        true
    }
}

impl BestFitTree {
    fn build_tree(&mut self) {
        self.tree.clear();

        let mut offset: u32 = 0;
        while offset < SEGMENT_SIZE as u32 {
            if !self.data()[offset as usize] {
                // If bit is 0, it's free
                let start_offset = offset;
                let mut current_size: u32 = 0;
                while offset < SEGMENT_SIZE as u32 && !self.data()[offset as usize] {
                    current_size += 1;
                    offset += 1;
                }
                self.insert_free_segment(current_size, start_offset);
            } else {
                offset += 1;
            }
        }
    }

    fn insert_free_segment(&mut self, size: u32, offset: u32) {
        self.tree.entry(size).or_insert_with(Vec::new).push(offset);
    }

    fn remove_free_segment(&mut self, size: u32, offset: u32) {
        if let Some(offsets) = self.tree.get_mut(&size) {
            if let Some(index) = offsets.iter().position(|&seg_offset| seg_offset == offset) {
                offsets.remove(index);
                if offsets.is_empty() {
                    self.tree.remove(&size);
                }
            }
        }
    }
}
