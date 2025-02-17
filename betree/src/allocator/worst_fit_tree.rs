use super::*;

/// Based on the free-space-map allocator from postgresql:
/// https://github.com/postgres/postgres/blob/02ed3c2bdcefab453b548bc9c7e0e8874a502790/src/backend/storage/freespace/README
pub struct WorstFitTree {
    data: BitArr!(for SEGMENT_SIZE, in u8, Lsb0),
    fsm_tree: Vec<(u32, u32)>, // Array to represent the FSM tree, storing max free space
    tree_height: u32,
}

impl Allocator for WorstFitTree {
    fn data(&mut self) -> &mut BitArr!(for SEGMENT_SIZE, in u8, Lsb0) {
        &mut self.data
    }

    /// Constructs a new `WorstFitFSM` given the segment allocation bitmap.
    /// The `bitmap` must have a length of `SEGMENT_SIZE`.
    fn new(bitmap: [u8; SEGMENT_SIZE_BYTES]) -> Self {
        let data = BitArray::new(bitmap);
        let mut allocator = WorstFitTree {
            data,
            fsm_tree: Vec::new(),
            tree_height: 0,
        };
        allocator.build_fsm_tree();
        allocator
    }

    fn allocate(&mut self, size: u32) -> Option<u32> {
        if size == 0 {
            return Some(0);
        }

        if self.fsm_tree.is_empty() || self.fsm_tree[0].1 < size {
            return None; // Not enough free space
        }

        let mut current_node_index = 0;
        while current_node_index < self.fsm_tree.len() / 2 {
            // Traverse internal nodes
            let left_child_index = 2 * current_node_index + 1;
            let right_child_index = 2 * current_node_index + 2;

            let left_child_value = *self.fsm_tree.get(left_child_index).unwrap_or(&(0, 0));
            let right_child_value = *self.fsm_tree.get(right_child_index).unwrap_or(&(0, 0));

            match (left_child_value.1 >= size, right_child_value.1 >= size) {
                (true, true) => {
                    // Both children can fit size
                    if left_child_value.1 < right_child_value.1 {
                        // Right is worse fit
                        current_node_index = right_child_index;
                    } else {
                        // Left is worse or equal fit
                        current_node_index = left_child_index;
                    }
                }
                (true, false) => current_node_index = left_child_index, // Only left can fit
                (false, true) => current_node_index = right_child_index, // Only right can fit
                (false, false) => unreachable!(), // Neither child can fit, stop traversal
            }
        }

        // current_node_index is now the index of the worst-fit leaf node
        assert!(current_node_index >= self.fsm_tree.len() / 2);
        let (offset, segment_size) = self.fsm_tree[current_node_index];

        assert!(segment_size >= size);

        self.mark(offset, size, Action::Allocate);

        // Update the segment in the leaf node
        self.fsm_tree[current_node_index].0 += size;
        self.fsm_tree[current_node_index].1 -= size;

        // Update internal nodes up to the root
        let mut current_index = current_node_index;
        while current_index > 0 {
            current_index = (current_index - 1) / 2; // Index of parent node
            let left_child_index = 2 * current_index + 1;
            let right_child_index = 2 * current_index + 2;

            let left_child_value = *self.fsm_tree.get(left_child_index).unwrap_or(&(0, 0));
            let right_child_value = *self.fsm_tree.get(right_child_index).unwrap_or(&(0, 0));
            if left_child_value.1 > right_child_value.1 {
                self.fsm_tree[current_index] = left_child_value
            } else {
                self.fsm_tree[current_index] = right_child_value
            }
        }

        return Some(offset);
    }

    fn allocate_at(&mut self, size: u32, offset: u32) -> bool {
        // Because the tree is sorted by offset because of how it's build, this shouldn't be to
        // hard to implement efficiently
        todo!()
    }
}

impl WorstFitTree {
    fn get_free_segments(&mut self) -> Vec<(u32, u32)> {
        let mut offset: u32 = 0;
        let mut free_segments = Vec::new();
        while offset < SEGMENT_SIZE as u32 {
            if !self.data()[offset as usize] {
                // If bit is 0, it's free
                let start_offset = offset;
                let mut current_size: u32 = 0;
                while offset < SEGMENT_SIZE as u32 && !self.data()[offset as usize] {
                    current_size += 1;
                    offset += 1;
                }
                free_segments.push((start_offset, current_size));
            } else {
                offset += 1;
            }
        }
        free_segments
    }

    fn build_fsm_tree(&mut self) {
        let leaf_nodes = self.get_free_segments();
        let leaf_nodes_num = leaf_nodes.len();

        if leaf_nodes_num == 0 {
            self.fsm_tree = vec![(0, 0)]; // Root node with 0 free space
            return;
        }

        // Calculate the size of the FSM tree array. For simplicity we assume complete tree for now.
        self.tree_height = (leaf_nodes_num as f64).log2().ceil() as u32;
        // Number of nodes in complete binary tree of height h is 2^(h+1) - 1
        let tree_nodes_num = (1 << (self.tree_height + 1)) - 1;

        self.fsm_tree.clear();
        self.fsm_tree.resize(tree_nodes_num as usize, (0, 0));

        // 1. Initialize leaf nodes in fsm_tree from free_segments
        // OPTIM: just use memcpy
        for (i, &(offset, size)) in leaf_nodes.iter().enumerate() {
            // Leaf nodes are at the end of the fsm_tree array in a complete binary tree
            let leaf_index = (tree_nodes_num / 2) + i;
            if leaf_index < tree_nodes_num {
                // Prevent out-of-bounds access if free_segments.len() is not power of 2
                self.fsm_tree[leaf_index] = (offset, size);
            }
        }

        // 2. Build internal nodes bottom-up similar to a binary heap
        for i in (0..(tree_nodes_num / 2)).rev() {
            let left_child_index = 2 * i + 1;
            let right_child_index = 2 * i + 2;

            // Default to 0 if index is out of bounds (incomplete tree)
            let left_child_value = *self.fsm_tree.get(left_child_index).unwrap_or(&(0, 0));
            let right_child_value = *self.fsm_tree.get(right_child_index).unwrap_or(&(0, 0));

            if left_child_value.1 > right_child_value.1 {
                self.fsm_tree[i] = left_child_value
            } else {
                self.fsm_tree[i] = right_child_value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_empty() {
        let bitmap = [0u8; SEGMENT_SIZE_BYTES];
        let allocator = WorstFitTree::new(bitmap);

        // In an empty bitmap, the root node should have a large free space
        assert_eq!(allocator.fsm_tree[0].0, 0 as u32);
        assert_eq!(allocator.fsm_tree[0].1, SEGMENT_SIZE as u32);
        assert_eq!(allocator.tree_height, 0);
    }

    #[test]
    fn build_simple() {
        // Example bitmap: 3 segments allocated at the beginning, 2 free, 3 allocated, rest free
        let mut allocator = WorstFitTree::new([0u8; SEGMENT_SIZE_BYTES]);
        let bitmap = allocator.data();

        // Manually allocate some segments
        bitmap[0..3].fill(true); // Allocate 3 blocks at the beginning
        bitmap[5..7].fill(true); // Allocate 2 blocks after the free ones

        let mut allocator = WorstFitTree::new(bitmap.into_inner());

        let fsm_tree = vec![
            (7, SEGMENT_SIZE as u32 - 7),
            (3, 2),
            (7, SEGMENT_SIZE as u32 - 7),
        ];
        assert_eq!(allocator.fsm_tree, fsm_tree);
        assert_eq!(allocator.tree_height, 1);
    }

    #[test]
    fn build_complex() {
        let mut allocator = WorstFitTree::new([0u8; SEGMENT_SIZE_BYTES]);
        let bitmap = allocator.data();

        // Manually allocate some segments to create a non-trivial tree
        bitmap[0..3].fill(true);
        bitmap[5..8].fill(true);
        bitmap[8..10].fill(true);
        bitmap[14..22].fill(true);
        bitmap[35..36].fill(true);
        bitmap[42..53].fill(true);

        let allocator = WorstFitTree::new(bitmap.into_inner());

        // binary heap layout
        let fsm_tree = vec![
            (53, SEGMENT_SIZE as u32 - 53),
            (22, 13),
            (53, SEGMENT_SIZE as u32 - 53),
            (10, 4),
            (22, 13),
            (53, SEGMENT_SIZE as u32 - 53),
            (0, 0),
            (3, 2),
            (10, 4),
            (22, 13),
            (36, 6),
            (53, SEGMENT_SIZE as u32 - 53),
            (0, 0),
            (0, 0),
            (0, 0),
        ];

        assert_eq!(fsm_tree, allocator.fsm_tree);
        assert_eq!(allocator.tree_height, 3);
    }

    #[test]
    fn allocate_empty_fsm_tree() {
        let bitmap = [0u8; SEGMENT_SIZE_BYTES];
        let mut allocator = WorstFitTree::new(bitmap);

        let allocation = allocator.allocate(1024);
        assert!(allocation.is_some()); // Allocation should succeed

        let allocated_offset = allocation.unwrap();
        assert_eq!(allocated_offset, 0); // Should allocate at the beginning

        // Check if the allocated region is marked as used in the bitmap
        assert!(allocator.data()[0..1024 as usize].all());
        // Check root node value after allocation
        assert_eq!(allocator.fsm_tree[0], (1024, SEGMENT_SIZE as u32 - 1024));
    }

    #[test]
    fn allocate_complex_fsm_tree() {
        let mut allocator = WorstFitTree::new([0u8; SEGMENT_SIZE_BYTES]);
        let bitmap = allocator.data();

        // Manually allocate some segments to create a non-trivial tree
        bitmap[0..3].fill(true);
        bitmap[5..8].fill(true);
        bitmap[8..10].fill(true);
        bitmap[14..22].fill(true);
        bitmap[35..36].fill(true);
        bitmap[42..53].fill(true);

        let mut allocator = WorstFitTree::new(bitmap.into_inner());

        // Worst-fit should allocate from the segment at offset 3 with size 2
        let allocation = allocator.allocate(2); // Request allocation of size 2
        assert!(allocation.is_some());
        assert_eq!(allocation.unwrap(), 53);
        // Verify that the allocated region is marked in the bitmap
        assert!(allocator.data()[53..55].all());

        let allocation2 = allocator.allocate(10);
        assert!(allocation2.is_some());
        assert_eq!(allocation2.unwrap(), 55);
        assert!(allocator.data()[55..65].all());

        // Allocate again, to use the next worst fit segment
        let allocation2 = allocator.allocate(100);
        assert!(allocation2.is_some());
        assert_eq!(allocation2.unwrap(), 65);
        assert!(allocator.data()[65..165].all());
        assert_eq!(allocator.fsm_tree[0].1, SEGMENT_SIZE as u32 - 165);
    }

    #[test]
    fn allocate_fail_fsm_tree() {
        let mut allocator = WorstFitTree::new([0u8; SEGMENT_SIZE_BYTES]);
        let root_free_space = allocator.fsm_tree[0].1;

        // Try to allocate more than available space
        let allocation = allocator.allocate(root_free_space + 1);
        assert!(allocation.is_none()); // Allocation should fail

        // Check if fsm_tree root value is still the same
        assert_eq!(allocator.fsm_tree[0].1, root_free_space); // Should remain unchanged
    }
}
