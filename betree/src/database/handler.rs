use super::{
    errors::*,
    root_tree_msg::{deadlist, segment, space_accounting},
    AtomicStorageInfo, DatasetId, DeadListData, Generation, StorageInfo, TreeInner,
};
use crate::{
    allocator::*,
    atomic_option::AtomicOption,
    cow_bytes::SlicedCowBytes,
    data_management::{CopyOnWriteEvent, Dml, HasStoragePreference, ObjectReference},
    storage_pool::{DiskOffset, GlobalDiskId},
    tree::{DefaultMessageAction, Node, Tree, TreeLayer},
    vdev::Block,
};
use owning_ref::OwningRef;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use seqlock::SeqLock;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// Returns a message for updating the allocation bitmap.
pub fn update_allocation_bitmap_msg(
    offset: DiskOffset,
    size: Block<u32>,
    action: Action,
) -> SlicedCowBytes {
    let segment_offset = SegmentId::get_block_offset(offset);
    DefaultMessageAction::upsert_bits_msg(segment_offset, size.0, action.as_bool())
}

pub fn update_storage_info(info: &StorageInfo) -> Option<SlicedCowBytes> {
    bincode::serialize(info)
        .ok()
        .as_ref()
        .map(|bytes| DefaultMessageAction::upsert_msg(0, bytes))
}

/// The database handler, holding management data for interactions
/// between the database and data management layers.
pub struct Handler<OR: ObjectReference> {
    // The version of the root tree which is initially present at the start of the process.
    pub(crate) root_tree_inner: AtomicOption<Arc<TreeInner<OR, DefaultMessageAction>>>,
    // An updated version of the root tree from this session, created after first sync.
    pub(crate) root_tree_snapshot: RwLock<Option<TreeInner<OR, DefaultMessageAction>>>,
    pub(crate) current_generation: SeqLock<Generation>,
    // Free Space counted as blocks
    pub(crate) free_space: HashMap<GlobalDiskId, AtomicStorageInfo>,
    pub(crate) free_space_tier: Vec<AtomicStorageInfo>,
    pub(crate) delayed_messages: Mutex<Vec<(Box<[u8]>, SlicedCowBytes)>>,
    pub(crate) last_snapshot_generation: RwLock<HashMap<DatasetId, Generation>>,
    pub(crate) allocator: AllocatorType,
    // Cache for allocators which have been in use since the last sync. This is
    // done to avoid cyclical updates on evictions.
    // NOTE: This map needs to be updated/emptied on sync's as the internal
    // representation is not updated on deallocation to avoid overwriting
    // potentially valid fallback data.
    pub(crate) allocators: RwLock<HashMap<SegmentId, RwLock<Box<dyn Allocator>>>>,
    pub(crate) allocations: AtomicU64,
    pub(crate) old_root_allocation: SeqLock<Option<(DiskOffset, Block<u32>)>>,
}

impl<OR: ObjectReference + HasStoragePreference> Handler<OR> {
    fn current_root_tree<'a, X>(&'a self, dmu: &'a X) -> impl TreeLayer<DefaultMessageAction> + 'a
    where
        X: Dml<Object = Node<OR>, ObjectRef = OR, ObjectPointer = OR::ObjectPointer>,
    {
        Tree::from_inner(
            self.root_tree_inner.get().unwrap().as_ref(),
            dmu,
            false,
            super::ROOT_TREE_STORAGE_PREFERENCE,
        )
    }

    fn last_root_tree<'a, X>(
        &'a self,
        dmu: &'a X,
    ) -> Option<impl TreeLayer<DefaultMessageAction> + 'a>
    where
        X: Dml<Object = Node<OR>, ObjectRef = OR, ObjectPointer = OR::ObjectPointer>,
    {
        OwningRef::new(self.root_tree_snapshot.read())
            .try_map(|lock| lock.as_ref().ok_or(()))
            .ok()
            .map(|inner| Tree::from_inner(inner, dmu, false, super::ROOT_TREE_STORAGE_PREFERENCE))
    }

    pub(super) fn bump_generation(&self) {
        self.allocators.write().clear();
        self.current_generation.lock_write().0 += 1;
    }
}

pub struct AllocatorGuard<'a> {
    inner: RwLockReadGuard<'a, HashMap<SegmentId, RwLock<Box<dyn Allocator>>>>,
    id: SegmentId,
}

impl<'a> AllocatorGuard<'a> {
    pub fn access(&self) -> RwLockWriteGuard<Box<dyn Allocator>> {
        self.inner.get(&self.id).unwrap().write()
    }
}

impl<OR: ObjectReference + HasStoragePreference> Handler<OR> {
    pub fn current_generation(&self) -> Generation {
        self.current_generation.read()
    }

    pub fn update_allocation_bitmap<X>(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        action: Action,
        dmu: &X,
    ) -> Result<()>
    where
        X: Dml<Object = Node<OR>, ObjectRef = OR, ObjectPointer = OR::ObjectPointer>,
    {
        self.allocations.fetch_add(1, Ordering::Release);
        let id = SegmentId::get(offset);
        let key = segment::id_to_key(id);
        let disk_key = offset.class_disk_id();
        let msg = update_allocation_bitmap_msg(offset, size, action);
        // NOTE: We perform double the amount of atomics here than necessary, but we do this for now to avoid reiteration
        match action {
            Action::Deallocate => {
                self.free_space
                    .get(&disk_key)
                    .expect("Could not find disk id in storage class")
                    .free
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
                self.free_space_tier[offset.storage_class() as usize]
                    .free
                    .fetch_add(size.as_u64(), Ordering::Relaxed);
            }
            Action::Allocate => {
                self.free_space
                    .get(&disk_key)
                    .expect("Could not find disk id in storage class")
                    .free
                    .fetch_sub(size.as_u64(), Ordering::Relaxed);
                self.free_space_tier[offset.storage_class() as usize]
                    .free
                    .fetch_sub(size.as_u64(), Ordering::Relaxed);
            }
        };

        let mut delayed_msgs = self.delayed_messages.lock();
        delayed_msgs.push((key.into(), msg));
        delayed_msgs.push((
            space_accounting::key(disk_key).into(),
            update_storage_info(&self.free_space.get(&disk_key).unwrap().into()).unwrap(),
        ));
        Ok(())
    }

    pub fn get_allocation_bitmap<X>(&self, id: SegmentId, dmu: &X) -> Result<AllocatorGuard>
    where
        X: Dml<Object = Node<OR>, ObjectRef = OR, ObjectPointer = OR::ObjectPointer>,
    {
        {
            // Test if bitmap is already in cache
            let foo = self.allocators.read();
            if foo.contains_key(&id) {
                return Ok(AllocatorGuard { inner: foo, id });
            }
        }

        let now = std::time::Instant::now();
        let mut bitmap = [0u8; SEGMENT_SIZE_BYTES];

        let key = segment::id_to_key(id);
        let segment = self.current_root_tree(dmu).get(&key[..])?;
        log::info!(
            "fetched {:?} bitmap elements",
            segment.as_ref().map(|s| s.len())
        );

        if let Some(segment) = segment {
            assert!(segment.len() <= SEGMENT_SIZE_BYTES);
            bitmap[..segment.len()].copy_from_slice(&segment[..]);
        }

        if let Some(tree) = self.last_root_tree(dmu) {
            if let Some(old_segment) = tree.get(&key[..])? {
                for (w, old) in bitmap.iter_mut().zip(old_segment.iter()) {
                    *w |= *old;
                }
            }
        }

        let mut allocator: Box<dyn Allocator> = match self.allocator {
            AllocatorType::FirstFitScan => Box::new(FirstFitScan::new(bitmap)),
            AllocatorType::FirstFitList => Box::new(FirstFitList::new(bitmap)),
            AllocatorType::FirstFitTree => Box::new(FirstFitTree::new(bitmap)),
            AllocatorType::NextFitScan => Box::new(NextFitScan::new(bitmap)),
            AllocatorType::NextFitList => Box::new(NextFitList::new(bitmap)),
            AllocatorType::BestFitScan => Box::new(BestFitScan::new(bitmap)),
            AllocatorType::BestFitList => Box::new(BestFitList::new(bitmap)),
            AllocatorType::BestFitTree => Box::new(BestFitTree::new(bitmap)),
            AllocatorType::ApproximateBestFitTree => Box::new(ApproximateBestFitTree::new(bitmap)),
            AllocatorType::WorstFitScan => Box::new(WorstFitScan::new(bitmap)),
            AllocatorType::WorstFitList => Box::new(WorstFitList::new(bitmap)),
            AllocatorType::WorstFitTree => Box::new(WorstFitTree::new(bitmap)),
            AllocatorType::SegmentAllocator => Box::new(SegmentAllocator::new(bitmap)),
        };

        if let Some((offset, size)) = self.old_root_allocation.read() {
            if SegmentId::get(offset) == id {
                allocator.allocate_at(size.as_u32(), SegmentId::get_block_offset(offset));
            }
        }

        log::info!("requested allocation bitmap, took {:?}", now.elapsed());

        self.allocators.write().insert(id, RwLock::new(allocator));

        let foo = self.allocators.read();
        Ok(AllocatorGuard { inner: foo, id })
    }

    pub fn free_space_disk(&self, disk_id: GlobalDiskId) -> Option<StorageInfo> {
        self.free_space.get(&disk_id).map(|elem| elem.into())
    }

    pub fn free_space_tier(&self, class: u8) -> Option<StorageInfo> {
        self.free_space_tier
            .get(class as usize)
            .map(|elem| elem.into())
    }

    /// Marks blocks from removed objects to be removed if they are no longer needed.
    /// Checks for the existence of snapshots which included this data, if snapshots are found continue to hold this key as "dead" key.
    // copy on write is a bit of an unlucky name
    pub fn copy_on_write(
        &self,
        offset: DiskOffset,
        size: Block<u32>,
        generation: Generation,
        dataset_id: DatasetId,
    ) -> CopyOnWriteEvent {
        if self
            .last_snapshot_generation
            .read()
            .get(&dataset_id)
            .cloned()
            < Some(generation)
        {
            // Deallocate
            let id = SegmentId::get(offset);
            let key = &segment::id_to_key(id) as &[_];
            log::debug!(
                "Marked a block range {{ offset: {:?}, size: {:?} }} for deallocation",
                offset,
                size
            );
            let msg = update_allocation_bitmap_msg(offset, size, Action::Deallocate);
            // NOTE: Update free size on both positions
            self.free_space
                .get(&offset.class_disk_id())
                .expect("Could not fetch disk id from storage class")
                .free
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            self.free_space_tier[offset.storage_class() as usize]
                .free
                .fetch_add(size.as_u64(), Ordering::Relaxed);
            let mut delayed_msgs = self.delayed_messages.lock();
            delayed_msgs.push((key.into(), msg));
            delayed_msgs.push((
                Box::new(space_accounting::key(offset.class_disk_id())),
                update_storage_info(&self.free_space.get(&offset.class_disk_id()).unwrap().into())
                    .unwrap(),
            ));
            CopyOnWriteEvent::Removed
        } else {
            // Add to dead list
            let key = &deadlist::key(dataset_id, self.current_generation.read(), offset) as &[_];

            let data = DeadListData {
                birth: generation,
                size,
            }
            .pack()
            .unwrap();

            let msg = DefaultMessageAction::insert_msg(&data);
            self.delayed_messages.lock().push((key.into(), msg));
            CopyOnWriteEvent::Preserved
        }
    }
}
