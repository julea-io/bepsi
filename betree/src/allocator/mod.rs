use crate::{cow_bytes::CowBytes, storage_pool::DiskOffset, vdev::Block};
use bitvec::prelude::*;
use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

// OPTIM: For all the LIST variants of allocators: try to remove a free segment that can be created
// on allocation. There are two ways:
// 1. call remove on free_segments: this copies all the elements after the removed one, one place
//    to the front. This keeps the sorting of the array
// 2. swap remove the segment: swap the empty segment to the back and decrease the len by 1. This
//    breaks the sorting but is probably much faster
// The allocate_at function is called quite rarely (only on creation of the database and on loading
// the allocator bitmap from disk) and it's the only reason, why the free_segments are kept sorted
// anyway. So changing that function to respect this change could increase the performance.
//
// OPTIM: For all the LIST variants of allocators: When building the bitmap we can save the size of
// the largest free segment. This could save some entire list traversals but introduces one branch
// per allocation.
mod first_fit_scan;
pub use self::first_fit_scan::FirstFitScan;

mod next_fit_scan;
pub use self::next_fit_scan::NextFitScan;

mod best_fit_scan;
pub use self::best_fit_scan::BestFitScan;

mod worst_fit_scan;
pub use self::worst_fit_scan::WorstFitScan;

mod segment_allocator;
pub use self::segment_allocator::SegmentAllocator;

mod first_fit_list;
pub use self::first_fit_list::FirstFitList;

mod next_fit_list;
pub use self::next_fit_list::NextFitList;

mod best_fit_list;
pub use self::best_fit_list::BestFitList;

mod worst_fit_list;
pub use self::worst_fit_list::WorstFitList;

mod first_fit_tree;
pub use self::first_fit_tree::FirstFitTree;

mod best_fit_tree;
pub use self::best_fit_tree::BestFitTree;

mod approximate_best_fit_tree;
pub use self::approximate_best_fit_tree::ApproximateBestFitTree;

mod worst_fit_tree;
pub use self::worst_fit_tree::WorstFitTree;

/// 256KiB, so that `vdev::BLOCK_SIZE * SEGMENT_SIZE == 1GiB`
pub const SEGMENT_SIZE: usize = 1 << SEGMENT_SIZE_LOG_2;
/// Number of bytes required to store a segments allocation bitmap
pub const SEGMENT_SIZE_BYTES: usize = SEGMENT_SIZE / 8;
const SEGMENT_SIZE_LOG_2: usize = 18;
const SEGMENT_SIZE_MASK: usize = SEGMENT_SIZE - 1;

/// The `AllocatorType` enum represents different strategies for allocating blocks
/// of memory within a fixed-size segment.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")] // This will deserialize "firstfit" as FirstFit
pub enum AllocatorType {
    /// **First Fit Scan:**
    /// This allocator searches the segment from the beginning and allocates the
    /// first free block that is large enough to satisfy the request.
    FirstFitScan,

    /// **First Fit List:**
    /// This allocator builds an internal list of the free space in the segment
    /// and then searches from the beginning in that list and allocates the
    /// first free block that is large enough to satisfy the request.
    FirstFitList,

    /// **First Fit Tree:**
    /// This allocator builds a binary tree of offsets and sizes, that has the
    /// max-heap property on the sizes and uses it to find suitable free space.
    FirstFitTree,

    /// **Next Fit Scan:**
    /// This allocator starts searching from the last allocation and continues
    /// searching the segment for the next free block that is large enough.
    NextFitScan,

    /// **Next Fit List:**
    /// This allocator builds an internal list of the free space in the segment
    /// and then starts from the last allocation in that list and allocates the
    /// next free block that is large enough to satisfy the request.
    NextFitList,

    /// **Best Fit Scan:**
    /// This allocator searches the entire segment and allocates the smallest
    /// free block that is large enough to satisfy the request. This simple
    /// version uses a linear search to find the best fit.
    BestFitScan,

    /// **Best Fit List:**
    /// This allocator maintains a list of free segments and chooses the best-fit
    /// segment from this list to allocate memory.
    BestFitList,

    /// **Best Fit Tree:**
    /// This allocator builds a btree of offsets and sizes, that is sorted by
    /// the sizes and uses it to find suitable free space.
    BestFitTree,

    /// **Approximate Best Fit Tree:**
    /// This allocator builds a binary tree of offsets and sizes, that has the
    /// max-heap property on the sizes and uses it to find suitable free space.
    ApproximateBestFitTree,

    /// **Worst Fit Scan:**
    /// This allocator searches the entire segment and allocates the largest
    /// free block. This simple version uses a linear search to find the worst
    /// fit.
    WorstFitScan,

    /// **Worst Fit List:**
    /// This allocator maintains a list of free segments and chooses the worst-fit
    /// (largest) segment from this list to allocate memory.
    WorstFitList,

    /// **Worst Fit Tree:**
    /// This allocator builds a binary tree of offsets and sizes, that has the
    /// max-heap property on the sizes and uses it to find suitable free space.
    WorstFitTree,

    /// **Segment Allocator:**
    /// This is a first fit allocator that was used before making the allocators
    /// generic. It is not efficient and mainly included for reference.
    SegmentAllocator,
}

/// The `Allocator` trait defines an interface for allocating and deallocating
/// blocks of memory within a fixed-size segment. Different allocators can
/// implement various strategies for managing free space within the segment.
pub trait Allocator: Send + Sync {
    /// Accesses the underlying bitmap data.
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0);

    /// Constructs a new `Allocator` instance given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self
    where
        Self: Sized;

    /// Allocates a block of memory of the given `size`.
    ///
    /// This method attempts to find a contiguous free block of memory within the
    /// segment that is large enough to satisfy the request.
    fn allocate(&mut self, size: u32) -> Option<u32>;

    /// Allocates a block of memory of the given `size` at the specified `offset`.
    ///
    /// This method attempts to allocate a contiguous block of memory at the given offset.
    /// TODO: investigate if we need this method at all
    fn allocate_at(&mut self, size: u32, offset: u32) -> bool;

    /// Marks a range of bits in the bitmap with the given action.
    #[inline]
    fn mark(&mut self, offset: u32, size: u32, action: Action) {
        let start_idx = offset as usize;
        let end_idx = (offset + size) as usize;
        let range = &mut self.data()[start_idx..end_idx];

        match action {
            Action::Allocate => debug_assert!(!range.any()),
            Action::Deallocate => debug_assert!(range.all()),
        }

        range.fill(action.as_bool());
    }
}

// TODO better wording
/// Allocation action
#[derive(Clone, Copy)]
pub enum Action {
    /// Deallocate an allocated block.
    Deallocate,
    /// Allocate a deallocated block.
    Allocate,
}

impl Action {
    /// Returns 1 if allocation and 0 if deallocation.
    #[inline]
    pub fn as_bool(self) -> bool {
        match self {
            Action::Deallocate => false,
            Action::Allocate => true,
        }
    }
}

/// Identifier for 1GiB segments of a `StoragePool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub u64);

impl SegmentId {
    /// Returns the corresponding segment of the given disk offset.
    pub fn get(offset: DiskOffset) -> Self {
        SegmentId(offset.as_u64() & !(SEGMENT_SIZE_MASK as u64))
    }

    /// Returns the block offset into the segment.
    pub fn get_block_offset(offset: DiskOffset) -> u32 {
        offset.as_u64() as u32 & SEGMENT_SIZE_MASK as u32
    }

    /// Returns the disk offset at the start of this segment.
    pub fn as_disk_offset(&self) -> DiskOffset {
        DiskOffset::from_u64(self.0)
    }

    /// Returns the disk offset of the block in this segment at the given
    /// offset.
    pub fn disk_offset(&self, segment_offset: u32) -> DiskOffset {
        DiskOffset::from_u64(self.0 + u64::from(segment_offset))
    }

    /// Returns the key of this segment for messages and queries.
    pub fn key(&self, key_prefix: &[u8]) -> CowBytes {
        // Shave off the two lower bytes because they are always null.
        let mut segment_key = [0; 8];
        BigEndian::write_u64(&mut segment_key[..], self.0);
        assert_eq!(&segment_key[6..], &[0, 0]);

        let mut key = CowBytes::new();
        key.push_slice(key_prefix);
        key.push_slice(&segment_key[..6]);
        key
    }

    /// Returns the ID of the disk that belongs to this segment.
    pub fn disk_id(&self) -> u16 {
        self.as_disk_offset().disk_id()
    }

    /// Returns the next segment ID.
    /// Wraps around at the end of the disk.
    pub fn next(&self, disk_size: Block<u64>) -> SegmentId {
        let disk_offset = self.as_disk_offset();
        if disk_offset.block_offset().as_u64() + SEGMENT_SIZE as u64 >= disk_size.as_u64() {
            SegmentId::get(DiskOffset::new(
                disk_offset.storage_class(),
                disk_offset.disk_id(),
                Block(0),
            ))
        } else {
            SegmentId(self.0 + SEGMENT_SIZE as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_id() {
        let offset = DiskOffset::new(1, 2, Block::from_bytes(4096));
        let segment = SegmentId::get(offset);

        assert_eq!(segment.as_disk_offset().storage_class(), 1);
        assert_eq!(segment.disk_id(), 2);
        assert_eq!(
            segment.as_disk_offset().block_offset(),
            Block::from_bytes(0)
        );
        assert_eq!(SegmentId::get_block_offset(offset), 1);
    }
}
