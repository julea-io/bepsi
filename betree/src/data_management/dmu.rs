use super::{
    cache_value::{CacheValueRef, TaggedCacheValue},
    errors::*,
    impls::{ModifiedObjectId, ObjRef, ObjectKey},
    object_ptr::ObjectPointer,
    CopyOnWriteEvent, Dml, HasStoragePreference, Object, ObjectReference,
};
use crate::{
    allocator::{Action, SegmentId, SEGMENT_SIZE},
    buffer::Buf,
    cache::{Cache, ChangeKeyError, RemoveError},
    checksum::{Builder, Checksum, State},
    compression::CompressionBuilder,
    data_management::CopyOnWriteReason,
    database::{DatasetId, Generation, Handler},
    migration::DmlMsg,
    size::{Size, SizeMut, StaticSize},
    storage_pool::{DiskOffset, StoragePoolLayer, NUM_STORAGE_CLASSES},
    tree::{Node, PivotKey},
    vdev::{Block, File, BLOCK_SIZE},
    StoragePreference,
};
use byteorder::{LittleEndian, WriteBytesExt};
use crossbeam_channel::Sender;
use futures::{executor::block_on, future::ok, prelude::*};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    arch::x86_64::{__rdtscp, _rdtsc},
    collections::HashMap,
    fs::OpenOptions,
    io::{BufWriter, Write},
    mem::replace,
    ops::DerefMut,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::yield_now,
};

/// The Data Management Unit.
pub struct Dmu<E: 'static, SPL: StoragePoolLayer>
where
    SPL::Checksum: StaticSize,
{
    default_compression: Box<dyn CompressionBuilder>,
    // NOTE: Why was this included in the first place? Delayed Compression? Streaming Compression?
    // default_compression_state: C::CompressionState,
    default_storage_class: u8,
    default_checksum_builder: <SPL::Checksum as Checksum>::Builder,
    alloc_strategy: [[Option<u8>; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES],
    pool: SPL,
    cache: RwLock<E>,
    written_back: Mutex<HashMap<ModifiedObjectId, ObjectPointer<SPL::Checksum>>>,
    modified_info: Mutex<HashMap<ModifiedObjectId, DatasetId>>,
    storage_hints: Arc<Mutex<HashMap<PivotKey, StoragePreference>>>,
    handler: Handler<ObjRef<ObjectPointer<SPL::Checksum>>>,
    // NOTE: The semantic structure of this looks as this
    // Storage Pool Layers:
    //      Layer Disks:
    //          Tuple of SegmentIDs and their according Allocators
    allocation_data: Box<[Box<[Mutex<Option<SegmentId>>]>]>,
    next_modified_node_id: AtomicU64,
    next_disk_id: AtomicU64,
    report_tx: Option<Sender<DmlMsg>>,
    #[cfg(feature = "allocation_log")]
    allocation_log_file: Mutex<BufWriter<std::fs::File>>,
}

impl<E, SPL> Dmu<E, SPL>
where
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
{
    /// Returns a new `Dmu`.
    pub fn new(
        default_compression: Box<dyn CompressionBuilder>,
        default_checksum_builder: <SPL::Checksum as Checksum>::Builder,
        default_storage_class: u8,
        pool: SPL,
        alloc_strategy: [[Option<u8>; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES],
        cache: E,
        handler: Handler<ObjRef<ObjectPointer<SPL::Checksum>>>,
        #[cfg(feature = "allocation_log")] allocation_log_file_path: PathBuf,
    ) -> Self {
        let allocation_data = (0..pool.storage_class_count())
            .map(|class| {
                (0..pool.disk_count(class))
                    .map(|_| Mutex::new(None))
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        #[cfg(feature = "allocation_log")]
        let allocation_log_file = Mutex::new(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(allocation_log_file_path)
                .expect("Failed to create allocation log file"),
        ));

        Dmu {
            // default_compression_state: default_compression.new_compression().expect("Can't create compression state"),
            default_compression,
            default_storage_class,
            default_checksum_builder,
            alloc_strategy,
            pool,
            cache: RwLock::new(cache),
            written_back: Mutex::new(HashMap::new()),
            modified_info: Mutex::new(HashMap::new()),
            storage_hints: Arc::new(Mutex::new(HashMap::new())),
            handler,
            allocation_data,
            next_modified_node_id: AtomicU64::new(1),
            next_disk_id: AtomicU64::new(0),
            report_tx: None,
            #[cfg(feature = "allocation_log")]
            allocation_log_file,
        }
    }

    /// Returns the underlying handler.
    pub fn handler(&self) -> &Handler<ObjRef<ObjectPointer<SPL::Checksum>>> {
        &self.handler
    }

    /// Returns the underlying cache.
    pub fn cache(&self) -> &RwLock<E> {
        &self.cache
    }

    /// Returns the underlying storage pool.
    pub fn pool(&self) -> &SPL {
        &self.pool
    }

    /// Writes the global header for the allocation logging.
    pub fn write_global_header(&self) -> Result<(), Error> {
        #[cfg(feature = "allocation_log")]
        {
            let mut file = self.allocation_log_file.lock();

            // Number of storage classes
            file.write_u8(self.pool.storage_class_count())?;

            // Disks per class
            for class in 0..self.pool.storage_class_count() {
                let disk_count = self.pool.disk_count(class);
                file.write_u16::<LittleEndian>(disk_count)?;
            }

            // Segments per disk
            for class in 0..self.pool.storage_class_count() {
                for disk in 0..self.pool.disk_count(class) {
                    let segment_count = self.pool.size_in_blocks(class, disk);
                    file.write_u64::<LittleEndian>(segment_count.as_u64())?;
                }
            }

            // Blocks per segment (constant)
            file.write_u64::<LittleEndian>(SEGMENT_SIZE.try_into().unwrap())?;
        }

        Ok(())
    }
}

impl<E, SPL> Dmu<E, SPL>
where
    E: Cache<
        Key = ObjectKey<Generation>,
        Value = TaggedCacheValue<RwLock<Node<ObjRef<ObjectPointer<SPL::Checksum>>>>, PivotKey>,
    >,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
{
    /// Stealing an [ObjectRef] can have multiple effects.  First, the
    /// corresponding node is moved in cache to the [ObjectKey::Modified] state.
    /// Second, the passed [ObjectRef] is moved to the [ObjectRef::Modified]
    /// state, if it has been in the [ObjectRef::Unmodified] state before
    /// [Self::copy_on_write] is called on the [ObjectPointer] contained, if this is
    /// not the case the corresponding running [Self::handle_write_back] will handle
    /// deallocation on completion.
    fn steal(
        &self,
        or: &mut <Self as Dml>::ObjectRef,
        info: DatasetId,
    ) -> Result<Option<<Self as Dml>::CacheValueRefMut>, Error> {
        debug!("Stealing {or:?}");
        let mid = self.next_modified_node_id.fetch_add(1, Ordering::Relaxed);
        let pk = or.index().clone();
        let mid = ModifiedObjectId {
            id: mid,
            pref: or.correct_preference(),
        };
        let entry = {
            let mut cache = self.cache.write();
            let was_present = cache.force_change_key(&or.as_key(), ObjectKey::Modified(mid));
            if !was_present {
                return Ok(None);
            }
            self.modified_info.lock().insert(mid, info);
            cache.get(&ObjectKey::Modified(mid), false).unwrap()
        };
        let obj = CacheValueRef::write(entry);

        if let ObjRef::Unmodified(ptr, ..) = replace(or, ObjRef::Modified(mid, pk)) {
            self.copy_on_write(ptr, CopyOnWriteReason::Steal, or.index().clone());
        }
        Ok(Some(obj))
    }

    /// Will be called when `or` is not in cache but was modified.
    /// Resolves two cases:
    ///     - Previous write back (`Modified(_)`) Will change `or` to
    ///       `InWriteback(_)`.
    ///     - Previous eviction after write back (`InWriteback(_)`) Will change
    ///       `or` to `Unmodified(_)`.
    fn fix_or(&self, or: &mut <Self as Dml>::ObjectRef) {
        match or {
            ObjRef::Unmodified(..) => unreachable!(),
            ObjRef::Modified(mid, pk) => {
                debug!("{mid:?} moved to InWriteback");
                *or = ObjRef::InWriteback(*mid, pk.clone());
            }
            ObjRef::InWriteback(mid, pk) => {
                // The object must have been written back recently.
                debug!("{mid:?} moved to Unmodified");
                let ptr = self.written_back.lock().remove(mid).unwrap();
                *or = ObjRef::Unmodified(ptr, pk.clone());
            }
            ObjRef::Incomplete(..) => unreachable!(),
        }
    }

    fn copy_on_write(
        &self,
        obj_ptr: ObjectPointer<SPL::Checksum>,
        steal: CopyOnWriteReason,
        pivot_key: PivotKey,
    ) {
        let actual_size = self.pool.actual_size(
            obj_ptr.offset().storage_class(),
            obj_ptr.offset().disk_id(),
            obj_ptr.size(),
        );
        #[cfg(feature = "allocation_log")]
        {
            let mut file = self.allocation_log_file.lock();
            let _ = file.write_u8(Action::Deallocate.as_bool() as u8);
            let _ = file.write_u64::<LittleEndian>(obj_ptr.offset.as_u64());
            let _ = file.write_u32::<LittleEndian>(obj_ptr.size.as_u32());
            let _ = file.write_u64::<LittleEndian>(0);
            let _ = file.write_u64::<LittleEndian>(0);
        }
        if let (CopyOnWriteEvent::Removed, Some(tx), CopyOnWriteReason::Remove) = (
            self.handler.copy_on_write(
                obj_ptr.offset(),
                actual_size,
                obj_ptr.generation(),
                obj_ptr.info(),
            ),
            &self.report_tx,
            steal,
        ) {
            let _ = tx
                .send(DmlMsg::remove(obj_ptr.offset(), obj_ptr.size(), pivot_key))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }
    }

    /// Fetches synchronously an object from disk and inserts it into the
    /// cache.
    fn fetch(&self, op: &<Self as Dml>::ObjectPointer, pivot_key: PivotKey) -> Result<(), Error> {
        // FIXME: reuse decompression_state
        debug!("Fetching {op:?}");
        let mut decompression_state = op.decompression_tag().new_decompression()?;
        let offset = op.offset();
        let generation = op.generation();

        let compressed_data = self
            .pool
            .read(op.size(), op.offset(), op.checksum().clone())?;

        let object: Node<ObjRef<ObjectPointer<SPL::Checksum>>> = {
            let data = decompression_state.decompress(compressed_data)?;
            Object::unpack_at(op.offset(), op.info(), data.into_boxed_slice())?
        };
        let key = ObjectKey::Unmodified { offset, generation };
        self.insert_object_into_cache(key, TaggedCacheValue::new(RwLock::new(object), pivot_key));
        Ok(())
    }

    /// Fetches asynchronously an object from disk and inserts it into the
    /// cache.
    fn try_fetch_async(
        &self,
        op: &<Self as Dml>::ObjectPointer,
        pivot_key: PivotKey,
    ) -> Result<
        impl TryFuture<
                Ok = (
                    ObjectPointer<<SPL as StoragePoolLayer>::Checksum>,
                    Buf,
                    PivotKey,
                ),
                Error = Error,
            > + Send,
        Error,
    > {
        let ptr = op.clone();

        Ok(self
            .pool
            .read_async(op.size(), op.offset(), op.checksum().clone())?
            .map_err(Error::from)
            .and_then(move |data| ok((ptr, data, pivot_key))))
    }

    fn insert_object_into_cache(&self, key: ObjectKey<Generation>, mut object: E::Value) {
        let size = object.value_mut().get_mut().size();
        let mut cache = self.cache.write();
        if !cache.contains_key(&key) {
            cache.insert(key, object, size);
        }
    }

    fn evict(&self, mut cache: RwLockWriteGuard<E>) -> Result<(), Error> {
        // TODO we may need to evict multiple objects
        // Algorithm overview:
        // Find some evictable object
        // - unpinned
        // - not in writeback state
        // - can_be_evicted
        // If it's `Unmodified` -> done
        // Change its key (`Modified`) to `InWriteback`
        // Pin object
        // Unlock cache
        // Serialize, compress, checksum
        // Fetch generation
        // Allocate
        // Write out
        // Try to remove from cache
        // If ok -> done
        // If this fails, call copy_on_write as object has been modified again

        let evict_result = cache.evict(|&key, entry, cache_contains_key| {
            let object = entry.value_mut().get_mut();
            let can_be_evicted = match key {
                ObjectKey::InWriteback(_) => false,
                ObjectKey::Unmodified { .. } => true,
                ObjectKey::Modified(_) => object
                    .for_each_child(|or| {
                        let is_unmodified = loop {
                            if let ObjRef::Unmodified(..) = *or {
                                break true;
                            }
                            if cache_contains_key(&or.as_key()) {
                                break false;
                            }
                            self.fix_or(or);
                        };
                        if is_unmodified {
                            Ok(())
                        } else {
                            Err(())
                        }
                    })
                    .is_ok(),
            };
            if can_be_evicted {
                Some(object.size())
            } else {
                None
            }
        });
        let (key, mut object) = match evict_result {
            None => return Ok(()),
            Some((key, object)) => (key, object),
        };

        let mid = match key {
            ObjectKey::InWriteback(_) => unreachable!(),
            ObjectKey::Unmodified { .. } => return Ok(()),
            ObjectKey::Modified(mid) => mid,
        };

        let size = object.value_mut().get_mut().size();
        cache.insert(ObjectKey::InWriteback(mid), object, size);
        let entry = cache.get(&ObjectKey::InWriteback(mid), false).unwrap();

        let pk = entry.tag().clone();
        drop(cache);
        let object = CacheValueRef::write(entry);

        self.handle_write_back(object, mid, true, pk)?;
        Ok(())
    }

    fn handle_write_back(
        &self,
        mut object: <Self as Dml>::CacheValueRefMut,
        mid: ModifiedObjectId,
        evict: bool,
        pivot_key: PivotKey,
    ) -> Result<<Self as Dml>::ObjectPointer, Error> {
        let object_size = {
            #[cfg(debug_assertions)]
            {
                super::Size::checked_size(&*object).expect("Size calculation mismatch")
            }
            #[cfg(not(debug_assertions))]
            {
                super::Size::size(&*object)
            }
        };
        log::trace!("Entering write back of {:?}", &mid);

        if object_size > 4 * 1024 * 1024 {
            warn!("Writing back large object: {}", object.debug_info());
        }

        debug!("Estimated object size is {object_size} bytes");
        debug!("Using compression {:?}", &self.default_compression);
        let generation = self.handler.current_generation();
        // Use storage hints if available
        if let Some(pref) = self.storage_hints.lock().remove(&pivot_key) {
            object.set_system_storage_preference(pref);
        }
        let storage_preference = object.correct_preference();
        let storage_class = storage_preference
            .preferred_class()
            .unwrap_or(self.default_storage_class);

        let compression = &self.default_compression;
        let compressed_data = {
            // FIXME: cache this
            let mut state = compression.new_compression()?;
            let mut buf = crate::buffer::BufWrite::with_capacity(Block(128));
            {
                object.pack(&mut buf)?;
                drop(object);
            }
            state.finish(buf.into_buf())?
        };

        assert!(compressed_data.len() <= u32::max_value() as usize);
        let size = compressed_data.len();
        debug!("Compressed object size is {size} bytes");
        let size = Block(((size + BLOCK_SIZE - 1) / BLOCK_SIZE) as u32);
        assert!(size.to_bytes() as usize >= compressed_data.len());
        let offset = self.allocate(storage_class, size)?;
        assert_eq!(size.to_bytes() as usize, compressed_data.len());
        /*if size.to_bytes() as usize != compressed_data.len() {
            let mut v = compressed_data.into_vec();
            v.resize(size.to_bytes() as usize, 0);
            compressed_data = v.into_boxed_slice();
        }*/

        let info = self.modified_info.lock().remove(&mid).unwrap();

        let checksum = {
            let mut state = self.default_checksum_builder.build();
            state.ingest(compressed_data.as_ref());
            state.finish()
        };

        self.pool.begin_write(compressed_data, offset)?;

        let obj_ptr = ObjectPointer {
            offset,
            size,
            checksum,
            decompression_tag: compression.decompression_tag(),
            generation,
            info,
        };

        let was_present;
        {
            let mut cache = self.cache.write();
            // We can safely ignore pins.
            // If it's pinned, it must be a readonly request.
            was_present = if evict {
                cache.force_remove(&ObjectKey::InWriteback(mid), object_size)
            } else {
                cache.force_change_key(
                    &ObjectKey::InWriteback(mid),
                    ObjectKey::Unmodified {
                        offset: obj_ptr.offset(),
                        generation: obj_ptr.generation(),
                    },
                )
            };
            if was_present {
                debug!("Inserted {mid:?} into written_back");
                self.written_back.lock().insert(mid, obj_ptr.clone());
            }
        }

        if !was_present {
            // The object has been `stolen`.  Notify the handler.
            self.copy_on_write(obj_ptr.clone(), CopyOnWriteReason::Steal, pivot_key.clone());
            // NOTE: Since this position is immediately deallocated we report
            // here a write for the old position. This has to be done since we
            // can't modify the old position as the the tree ObjectRef will not
            // be updated until the next writeback when it is inserted in
            // written back. Otherwise we can't find the Cache value anymore
            // from the tree...  o.O
            if let Some(report_tx) = &self.report_tx {
                let _ = report_tx
                    .send(DmlMsg::write(obj_ptr.offset(), size, pivot_key))
                    .map_err(|_| warn!("Channel Receiver has been dropped."));
            }
        } else if let Some(report_tx) = &self.report_tx {
            let _ = report_tx
                .send(DmlMsg::write(obj_ptr.offset(), size, pivot_key))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }

        trace!("handle_write_back: Leaving");
        Ok(obj_ptr)
    }

    fn allocate(&self, storage_preference: u8, size: Block<u32>) -> Result<DiskOffset, Error> {
        assert!(storage_preference < NUM_STORAGE_CLASSES as u8);
        if size >= Block(2048) {
            warn!("Very large allocation requested: {:?}", size);
        }

        log::trace!(
            "Enter allocate: {{ storage_preference: {}, size: {:?} }}",
            storage_preference,
            size
        );

        let strategy = self.alloc_strategy[storage_preference as usize];

        // NOTE: Could we mark classes, disks and/or segments as full to prevent looping over them?
        // We would then also need to handle this, when deallocating things.
        // Would full mean completely full or just not having enough contiguous memory of some
        // size?
        // Or save the largest contiguous memory region as a value and compare against that. For
        // that the allocator needs to support that and we have to 'bubble' the largest value up.
        #[cfg(feature = "allocation_log")]
        let mut start_cycles_global = get_cycles();
        #[cfg(feature = "allocation_log")]
        let mut total_cycles_local: u64 = 0;
        'class: for &class in strategy.iter().flatten() {
            let disks_in_class = self.pool.disk_count(class);
            if disks_in_class == 0 {
                continue;
            }

            if self
                .handler
                .free_space_tier(class)
                .expect("Has to exist")
                .free
                .as_u64()
                < size.as_u64()
            {
                warn!(
                    "Storage tier {class} does not have enough space remaining. {} blocks of {}",
                    self.handler.free_space_tier(class).unwrap().free.as_u64(),
                    size.as_u64()
                );
                continue;
            }

            let start_disk_id = (self.next_disk_id.fetch_add(1, Ordering::Relaxed)
                % u64::from(disks_in_class)) as u16;
            let disk_id = (start_disk_id..disks_in_class)
                .chain(0..start_disk_id)
                .max_by_key(|&disk_id| {
                    self.pool.effective_free_size(
                        class,
                        disk_id,
                        self.handler
                            .free_space_disk(DiskOffset::construct_disk_id(class, disk_id))
                            .expect("We can be sure that this disk id exists.")
                            .free,
                    )
                })
                .unwrap();
            let size = self.pool.actual_size(class, disk_id, size);
            let disk_size = self.pool.size_in_blocks(class, disk_id);

            let disk_offset = {
                let mut last_seg_id = self.allocation_data[class as usize][disk_id as usize].lock();
                let segment_id = if last_seg_id.is_some() {
                    last_seg_id.as_mut().unwrap()
                } else {
                    let segment_id = SegmentId::get(DiskOffset::new(class, disk_id, Block(0)));
                    *last_seg_id = Some(segment_id);
                    last_seg_id.as_mut().unwrap()
                };

                let first_seen_segment_id = *segment_id;
                loop {
                    // Has to be split because else the temporary value is dropped while borrowing
                    let bitmap = self.handler.get_allocation_bitmap(*segment_id, self)?;
                    let mut allocator = bitmap.access();

                    #[cfg(not(feature = "allocation_log"))]
                    {
                        let allocation = allocator.allocate(size.as_u32());
                        if let Some(segment_offset) = allocation {
                            let disk_offset = segment_id.disk_offset(segment_offset);
                            break disk_offset;
                        }
                    }
                    #[cfg(feature = "allocation_log")]
                    {
                        let start_cycles_allocation = get_cycles();
                        let allocation = allocator.allocate(size.as_u32());
                        let end_cycles_allocation = get_cycles();
                        total_cycles_local += end_cycles_allocation - start_cycles_allocation;

                        if let Some(segment_offset) = allocation {
                            let disk_offset = segment_id.disk_offset(segment_offset);
                            let total_cycles_global = end_cycles_allocation - start_cycles_global;

                            let mut file = self.allocation_log_file.lock();
                            file.write_u8(Action::Allocate.as_bool() as u8)?;
                            file.write_u64::<LittleEndian>(disk_offset.as_u64())?;
                            file.write_u32::<LittleEndian>(size.as_u32())?;
                            file.write_u64::<LittleEndian>(total_cycles_local)?;
                            file.write_u64::<LittleEndian>(total_cycles_global)?;

                            break disk_offset;
                        }
                    }

                    let next_segment_id = segment_id.next(disk_size);
                    trace!(
                        "Next allocator segment: {:?} -> {:?} ({:?})",
                        segment_id,
                        next_segment_id,
                        disk_size,
                    );
                    if next_segment_id == first_seen_segment_id {
                        // Can't allocate in this class, try next
                        warn!("Allocation failed not enough space");
                        debug!(
                            "Free space is {:?} blocks",
                            self.handler.free_space_tier(class)
                        );
                        continue 'class;
                    }
                    *segment_id = next_segment_id;
                }
            };

            info!("Allocated {:?} at {:?}", size, disk_offset);
            debug!(
                "Remaining space is {:?} blocks",
                self.handler.free_space_tier(class)
            );
            self.handler
                .update_allocation_bitmap(disk_offset, size, Action::Allocate, self)?;

            return Ok(disk_offset);
        }

        warn!(
            "No available layer can provide enough free storage {:?}",
            size
        );
        Err(Error::OutOfSpaceError)
    }

    /// Tries to allocate `size` blocks at `disk_offset`.  Might fail if
    /// already in use.
    pub fn allocate_raw_at(&self, disk_offset: DiskOffset, size: Block<u32>) -> Result<(), Error> {
        let disk_id = disk_offset.disk_id();
        let num_disks = self.pool.num_disks(disk_offset.storage_class(), disk_id);
        let size = size * num_disks as u32;
        let segment_id = SegmentId::get(disk_offset);
        let mut x =
            self.allocation_data[disk_offset.storage_class() as usize][disk_id as usize].lock();
        let allocator = self.handler.get_allocation_bitmap(segment_id, self)?;
        if allocator
            .access()
            .allocate_at(size.as_u32(), SegmentId::get_block_offset(disk_offset))
        {
            *x = Some(segment_id);
            self.handler
                .update_allocation_bitmap(disk_offset, size, Action::Allocate, self)?;
            Ok(())
        } else {
            Err(Error::RawAllocationError {
                at: disk_offset,
                size,
            })
        }
    }

    /// Receives an appendable list of [ModifiedObjectId] which is filled with
    /// all modified children of this node.  The reference [ModifiedObjectId] is
    /// updated from [ObjectKey::Modified] to [ObjectKey::InWriteback] in the
    /// cache, if no children are found which need to be written first.
    /// Returns a [CacheValueRef] to this new key if succesful.
    fn prepare_write_back(
        &self,
        mid: ModifiedObjectId,
        dep_mids: &mut Vec<(ModifiedObjectId, PivotKey)>,
    ) -> Result<Option<<Self as Dml>::CacheValueRefMut>, ()> {
        trace!("prepare_write_back: Enter");
        loop {
            // trace!("prepare_write_back: Trying to acquire cache write lock");
            let mut cache = self.cache.write();
            // trace!("prepare_write_back: Acquired");
            if cache.contains_key(&ObjectKey::InWriteback(mid)) {
                // TODO wait
                drop(cache);
                yield_now();
                // trace!("prepare_write_back: Cache contained key, waiting..");
                continue;
            }
            let result =
                cache.change_key(&ObjectKey::Modified(mid), |_, entry, cache_contains_key| {
                    let object = entry.value_mut().get_mut();
                    let mut modified_children = false;
                    object
                        .for_each_child::<(), _>(|or| loop {
                            let mid = match or {
                                ObjRef::Unmodified(..) => break Ok(()),
                                ObjRef::InWriteback(mid, ..) | ObjRef::Modified(mid, ..) => *mid,
                                ObjRef::Incomplete(_) => unreachable!(),
                            };
                            let pk = or.index().clone();
                            if cache_contains_key(&or.as_key()) {
                                modified_children = true;
                                dep_mids.push((mid, pk));
                                break Ok(());
                            }
                            self.fix_or(or);
                        })
                        .unwrap();
                    if modified_children {
                        Err(())
                    } else {
                        Ok(ObjectKey::InWriteback(mid))
                    }
                });
            return match result {
                Ok(()) => Ok(Some(
                    cache
                        .get(&ObjectKey::InWriteback(mid), false)
                        .map(CacheValueRef::write)
                        .unwrap(),
                )),
                Err(ChangeKeyError::NotPresent) => Ok(None),
                Err(ChangeKeyError::Pinned) => {
                    // TODO wait
                    warn!("Pinned node");
                    drop(cache);
                    yield_now();
                    continue;
                }
                Err(ChangeKeyError::CallbackError(())) => Err(()),
            };
        }
    }
}

impl<E, SPL> super::Dml for Dmu<E, SPL>
where
    E: Cache<
        Key = ObjectKey<Generation>,
        Value = TaggedCacheValue<RwLock<Node<ObjRef<ObjectPointer<SPL::Checksum>>>>, PivotKey>,
    >,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
{
    type ObjectPointer = ObjectPointer<SPL::Checksum>;
    type ObjectRef = ObjRef<Self::ObjectPointer>;
    type Object = Node<Self::ObjectRef>;
    type CacheValueRef = CacheValueRef<E::ValueRef, RwLockReadGuard<'static, Self::Object>>;
    type CacheValueRefMut = CacheValueRef<E::ValueRef, RwLockWriteGuard<'static, Self::Object>>;
    type Spl = SPL;

    fn spl(&self) -> &Self::Spl {
        &self.pool
    }

    fn try_get(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRef> {
        let result = {
            // Drop order important
            let cache = self.cache.read();
            cache.get(&or.as_key(), false)
        };
        result.map(CacheValueRef::read)
    }

    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut> {
        if let ObjRef::Modified(..) = *or {
            let result = {
                let cache = self.cache.read();
                cache.get(&or.as_key(), true)
            };
            result.map(CacheValueRef::write)
        } else {
            None
        }
    }

    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, Error> {
        let mut cache = self.cache.read();
        loop {
            if let Some(entry) = cache.get(&or.as_key(), true) {
                drop(cache);
                return Ok(CacheValueRef::read(entry));
            }
            if let ObjRef::Unmodified(ref ptr, ref pk) = *or {
                drop(cache);

                self.fetch(ptr, pk.clone())?;
                if let Some(report_tx) = &self.report_tx {
                    let _ = report_tx
                        .send(DmlMsg::fetch(ptr.offset(), ptr.size(), pk.clone()))
                        .map_err(|_| warn!("Channel Receiver has been dropped."));
                }
                // Check if any storage hints are available and update the node.
                // This moves the object reference into the modified state.
                if let Some(pref) = self.storage_hints.lock().remove(pk) {
                    if let Some(mut obj) = self.steal(or, ptr.info())? {
                        obj.set_system_storage_preference(pref)
                    }
                }
                cache = self.cache.read();
            } else {
                self.fix_or(or);
            }
        }
    }

    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: DatasetId,
    ) -> Result<Self::CacheValueRefMut, Error> {
        // Fast path
        if let Some(obj) = self.try_get_mut(or) {
            return Ok(obj);
        }
        // Object either not mutable or not present.
        loop {
            // Try to steal it if present.
            if let Some(obj) = self.steal(or, info)? {
                return Ok(obj);
            }
            // Fetch it.
            self.get(or)?;
        }
    }

    fn insert(&self, object: Self::Object, info: DatasetId, pk: PivotKey) -> Self::ObjectRef {
        let mid = ModifiedObjectId {
            id: self.next_modified_node_id.fetch_add(1, Ordering::Relaxed),
            pref: object.correct_preference(),
        };
        self.modified_info.lock().insert(mid, info);
        let key = ObjectKey::Modified(mid);
        let size = object.size();
        self.cache.write().insert(
            key,
            TaggedCacheValue::new(RwLock::new(object), pk.clone()),
            size,
        );
        ObjRef::Modified(mid, pk)
    }

    fn insert_and_get_mut(
        &self,
        object: Self::Object,
        info: DatasetId,
        pk: PivotKey,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef) {
        let mid = ModifiedObjectId {
            id: self.next_modified_node_id.fetch_add(1, Ordering::Relaxed),
            pref: object.correct_preference(),
        };
        self.modified_info.lock().insert(mid, info);
        let key = ObjectKey::Modified(mid);
        let size = object.size();
        let entry = {
            let mut cache = self.cache.write();
            cache.insert(
                key,
                TaggedCacheValue::new(RwLock::new(object), pk.clone()),
                size,
            );
            cache.get(&key, false).unwrap()
        };
        (CacheValueRef::write(entry), ObjRef::Modified(mid, pk))
    }

    fn remove(&self, or: Self::ObjectRef) {
        match self.cache.write().remove(&or.as_key(), |obj| obj.size()) {
            Ok(_) | Err(RemoveError::NotPresent) => {}
            // TODO
            Err(RemoveError::Pinned) => unimplemented!(),
        };
        if let ObjRef::Unmodified(ref ptr, ..) = or {
            self.copy_on_write(ptr.clone(), CopyOnWriteReason::Remove, or.index().clone());
        }
    }

    fn get_and_remove(
        &self,
        mut or: Self::ObjectRef,
    ) -> Result<Node<ObjRef<ObjectPointer<SPL::Checksum>>>, Error> {
        let obj = loop {
            self.get(&mut or)?;
            match self.cache.write().remove(&or.as_key(), |obj| obj.size()) {
                Ok(obj) => break obj,
                Err(RemoveError::NotPresent) => {}
                // TODO
                Err(RemoveError::Pinned) => unimplemented!(),
            };
        };
        if let ObjRef::Unmodified(ref ptr, ..) = or {
            self.copy_on_write(ptr.clone(), CopyOnWriteReason::Remove, or.index().clone());
        }
        Ok(obj.into_value().into_inner())
    }

    fn root_ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef {
        Self::ObjectRef::root_ref_from_obj_ptr(r)
    }

    fn evict(&self) -> Result<(), Error> {
        // TODO shortcut without locking cache
        let cache = self.cache.write();
        if cache.size() > cache.capacity() {
            self.evict(cache)?;
        }
        Ok(())
    }

    fn verify_cache(&self) {
        self.cache.write().verify();
    }

    /// Trigger a write back of an entire subtree.  This is intended for use
    /// with a dataset root, though will function on any subtree specified if
    /// needed.  A write back on a subtree will always write the lowest modified
    /// node level first and then propagate writes upwards until the subtree
    /// root is reached.
    fn write_back<F, FO>(&self, mut acquire_or_lock: F) -> Result<Self::ObjectPointer, Error>
    where
        F: FnMut() -> FO,
        FO: DerefMut<Target = Self::ObjectRef>,
    {
        trace!("write_back: Enter");
        let (object, mid, mid_pk) = loop {
            trace!("write_back: Trying to acquire lock");
            let mut or = acquire_or_lock();
            trace!("write_back: Acquired lock");
            let mid = match &*or {
                ObjRef::Unmodified(ref p, ..) => return Ok(p.clone()),
                ObjRef::InWriteback(mid, ..) | ObjRef::Modified(mid, ..) => *mid,
                ObjRef::Incomplete(..) => unreachable!(),
            };
            let mut mids = Vec::new();
            trace!("write_back: Preparing write back");
            match self.prepare_write_back(mid, &mut mids) {
                Ok(None) => {
                    trace!("write_back: Was Ok(None)");
                    self.fix_or(&mut or)
                }
                Ok(Some(object)) => break (object, mid, or.index().clone()),
                Err(()) => {
                    trace!("write_back: Was Err");
                    drop(or);
                    while let Some((mid, mid_pk)) = mids.last().cloned() {
                        trace!("write_back: Trying to prepare write back");
                        match self.prepare_write_back(mid, &mut mids) {
                            Ok(None) => {}
                            Ok(Some(object)) => {
                                trace!("write_back: Was Ok Some");
                                self.handle_write_back(object, mid, false, mid_pk).map_err(
                                    |err| {
                                        let mut cache = self.cache.write();
                                        let _ = cache.change_key::<(), _>(
                                            &ObjectKey::InWriteback(mid),
                                            // Has to have been in the modified state before
                                            |_, _, _| Ok(ObjectKey::Modified(mid)),
                                        );
                                        err
                                    },
                                )?;
                            }
                            Err(()) => continue,
                        };
                        mids.pop();
                    }
                }
            }
        };
        trace!("write_back: Leave");

        self.handle_write_back(object, mid, false, mid_pk)
    }

    type Prefetch = Pin<
        Box<
            dyn Future<Output = Result<(<Self as Dml>::ObjectPointer, Buf, PivotKey), Error>>
                + Send
                + 'static,
        >,
    >;
    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, Error> {
        if self.cache.read().contains_key(&or.as_key()) {
            return Ok(None);
        }
        Ok(match *or {
            ObjRef::Modified(..) | ObjRef::InWriteback(..) => None,
            ObjRef::Unmodified(ref p, ref pk) => {
                Some(Box::pin(self.try_fetch_async(p, pk.clone())?.into_future()))
            }
            ObjRef::Incomplete(..) => unreachable!(),
        })
    }

    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<(), Error> {
        let (ptr, compressed_data, pk) = block_on(p)?;
        let object: Node<ObjRef<ObjectPointer<SPL::Checksum>>> = {
            let data = ptr
                .decompression_tag()
                .new_decompression()?
                .decompress(compressed_data)?;
            Object::unpack_at(ptr.offset(), ptr.info(), data.into_boxed_slice())?
        };
        let key = ObjectKey::Unmodified {
            offset: ptr.offset(),
            generation: ptr.generation(),
        };
        self.insert_object_into_cache(key, TaggedCacheValue::new(RwLock::new(object), pk.clone()));
        if let Some(report_tx) = &self.report_tx {
            let _ = report_tx
                .send(DmlMsg::fetch(ptr.offset(), ptr.size(), pk))
                .map_err(|_| warn!("Channel Receiver has been dropped."));
        }
        Ok(())
    }

    // Cache depending methods
    type CacheStats = E::Stats;

    fn cache_stats(&self) -> Self::CacheStats {
        self.cache.read().stats()
    }

    fn drop_cache(&self) {
        let mut cache = self.cache.write();
        let keys: Vec<_> = cache
            .iter()
            .cloned()
            .filter(|&key| matches!(key, ObjectKey::Unmodified { .. }))
            .collect();
        for key in keys {
            let _ = cache.remove(&key, |obj| obj.size());
        }
    }
}

impl<E, SPL> super::DmlWithHandler for Dmu<E, SPL>
where
    E: Cache<
        Key = ObjectKey<Generation>,
        Value = TaggedCacheValue<RwLock<Node<ObjRef<ObjectPointer<SPL::Checksum>>>>, PivotKey>,
    >,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
{
    type Handler = Handler<ObjRef<ObjectPointer<SPL::Checksum>>>;

    fn handler(&self) -> &Self::Handler {
        &self.handler
    }
}

impl<E, SPL> super::DmlWithStorageHints for Dmu<E, SPL>
where
    E: Cache<
        Key = ObjectKey<Generation>,
        Value = TaggedCacheValue<RwLock<Node<ObjRef<ObjectPointer<SPL::Checksum>>>>, PivotKey>,
    >,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
{
    fn storage_hints(&self) -> Arc<Mutex<HashMap<PivotKey, StoragePreference>>> {
        Arc::clone(&self.storage_hints)
    }

    fn default_storage_class(&self) -> StoragePreference {
        StoragePreference::from_u8(self.default_storage_class)
    }
}

impl<E, SPL> super::DmlWithReport for Dmu<E, SPL>
where
    E: Cache<
        Key = ObjectKey<Generation>,
        Value = TaggedCacheValue<RwLock<Node<ObjRef<ObjectPointer<SPL::Checksum>>>>, PivotKey>,
    >,
    SPL: StoragePoolLayer,
    SPL::Checksum: StaticSize,
{
    fn with_report(mut self, tx: Sender<DmlMsg>) -> Self {
        self.report_tx = Some(tx);
        self
    }

    fn set_report(&mut self, tx: Sender<DmlMsg>) {
        self.report_tx = Some(tx);
    }
}

fn get_cycles() -> u64 {
    unsafe {
        //let mut aux = 0;
        //__rdtscp(aux)
        _rdtsc()
    }
}
