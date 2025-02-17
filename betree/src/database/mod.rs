//! This module provides the Database Layer.
use crate::{
    allocator::*,
    atomic_option::AtomicOption,
    cache::ClockCache,
    checksum::GxHash,
    compression::CompressionConfiguration,
    cow_bytes::SlicedCowBytes,
    data_management::{
        self, Dml, DmlWithHandler, DmlWithReport, DmlWithStorageHints, Dmu, TaggedCacheValue,
    },
    metrics::{metrics_init, MetricsConfiguration},
    migration::{DatabaseMsg, DmlMsg, GlobalObjectId, MigrationPolicies},
    size::StaticSize,
    storage_pool::{
        DiskOffset, StoragePoolConfiguration, StoragePoolLayer, StoragePoolUnit,
        NUM_STORAGE_CLASSES,
    },
    tree::{
        DefaultMessageAction, ErasedTreeSync, Inner as TreeInner, Node, PivotKey, Tree, TreeLayer,
    },
    vdev::Block,
    StoragePreference,
};
use bincode::{deserialize, serialize_into};
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use crossbeam_channel::Sender;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use seqlock::SeqLock;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    iter::FromIterator,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

mod dataset;
pub(crate) mod errors;
mod handler;
pub(crate) mod root_tree_msg;
mod snapshot;
mod storage_info;
mod superblock;
mod sync_timer;

use root_tree_msg::{dataset as dataset_key, snapshot as snapshot_key, space_accounting};
use storage_info::AtomicStorageInfo;
pub use storage_info::StorageInfo;

#[cfg(feature = "figment_config")]
mod figment;

pub use self::{
    dataset::Dataset,
    errors::*,
    handler::{update_allocation_bitmap_msg, Handler},
    snapshot::Snapshot,
    superblock::Superblock,
};
const ROOT_DATASET_ID: DatasetId = DatasetId(0);
const ROOT_TREE_STORAGE_PREFERENCE: StoragePreference = StoragePreference::FASTEST;
const DEFAULT_CACHE_SIZE: usize = 256 * 1024 * 1024;
const DEFAULT_SYNC_INTERVAL_MS: u64 = 1000;

// This is the hash used overall in the entire database. For reconfiguration
// recompilation is necessary and this type changed.
type Checksum = GxHash;

type ObjectPointer = data_management::ObjectPointer<Checksum>;
pub(crate) type ObjectRef = data_management::impls::ObjRef<ObjectPointer>;
pub(crate) type Object = Node<ObjectRef>;
type DbHandler = Handler<ObjectRef>;

pub(crate) type RootSpu = StoragePoolUnit<Checksum>;
pub(crate) type RootDmu = Dmu<
    ClockCache<
        data_management::impls::ObjectKey<Generation>,
        TaggedCacheValue<RwLock<Object>, PivotKey>,
    >,
    RootSpu,
>;

pub(crate) type MessageTree<Dmu, Message> =
    Tree<Arc<Dmu>, Message, Arc<TreeInner<ObjectRef, Message>>>;

pub(crate) type RootTree<Dmu> = MessageTree<Dmu, DefaultMessageAction>;
pub(crate) type DatasetTree<Dmu> = RootTree<Dmu>;

/// This enum controls whether to search for an existing database to reuse,
/// and whether to create a new one if none could be found or.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessMode {
    /// Read and use an existing superblock, abort if none is found.
    OpenIfExists,
    /// Don't check for an existing superblock, create a new root tree and write it to disk.
    AlwaysCreateNew,
    /// Use an existing database if found, create a new one otherwise.
    OpenOrCreate,
}

/// Determines when sync is called
pub enum SyncMode {
    /// No automatic sync, only on user call
    Explicit,
    /// Every `interval_ms` milliseconds, sync is called
    Periodic { interval_ms: u64 },
}

/// A bundle type of component configuration types, used during [Database::build]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields, default)]
pub struct DatabaseConfiguration {
    /// Backing storage to be used
    pub storage: StoragePoolConfiguration,

    /// During allocation of a request with priority p, the allocator will
    /// try every class in alloc_strategy\[p\] in order.
    ///
    /// A few example strategies:
    /// - `[[0], [1], [2], [3]]` will allocate each request in the request class,
    ///     but doesn't allow falling back to other classes if an allocation fails
    /// - `[[0], [1], [1], [1]]` will allocate requests for 0 and 1 in the requested classes,
    ///     but maps 2 and 3 to 1 as well. Again, without fallback
    /// - `[[0, 1, 2, 3], [1, 2, 3], [2, 3], [3]]` will try to allocate as requested,
    ///     but allows falling back to higher classes if a class is full
    pub alloc_strategy: [Vec<u8>; NUM_STORAGE_CLASSES],
    /// Default storage class, used when attempting to allocate a tree object without
    /// a storage preference
    pub default_storage_class: u8,
    /// Which compression type to use, and the type-specific compression parameters
    pub compression: CompressionConfiguration,
    /// Size of cache in TODO
    pub cache_size: usize,
    /// Whether to check for and open an existing database, or overwrite it
    pub access_mode: AccessMode,

    /// When set, try to sync all datasets every `sync_interval_ms` milliseconds
    pub sync_interval_ms: Option<u64>,

    /// Set the migration policy to be used.
    pub migration_policy: Option<MigrationPolicies>,

    /// If and how to log database metrics
    pub metrics: Option<MetricsConfiguration>,

    /// Where to log the allocations
    pub allocation_log_file_path: PathBuf,

    /// Select the allocator to use
    pub allocator: AllocatorType,
}

impl Default for DatabaseConfiguration {
    fn default() -> Self {
        Self {
            storage: StoragePoolConfiguration::default(),
            // identity mapping
            alloc_strategy: [vec![0], vec![1], vec![2], vec![3]],
            default_storage_class: 0,
            compression: CompressionConfiguration::None,
            cache_size: DEFAULT_CACHE_SIZE,
            access_mode: AccessMode::OpenIfExists,
            sync_interval_ms: Some(DEFAULT_SYNC_INTERVAL_MS),
            metrics: None,
            migration_policy: None,
            allocation_log_file_path: PathBuf::from("allocation_log.bin"),
            allocator: AllocatorType::SegmentAllocator,
        }
    }
}

impl DatabaseConfiguration {
    /// Serialize the configuration to a given path in the json format.
    pub fn write_to_json<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        serde_json::to_writer_pretty(file, &self)?;
        Ok(())
    }
}

impl DatabaseConfiguration {
    pub fn new_spu(&self) -> Result<RootSpu> {
        Ok(StoragePoolUnit::<Checksum>::new(&self.storage)?)
    }

    pub fn new_handler(&self, spu: &RootSpu) -> DbHandler {
        Handler {
            root_tree_inner: AtomicOption::new(),
            root_tree_snapshot: RwLock::new(None),
            current_generation: SeqLock::new(Generation(1)),
            delayed_messages: Mutex::new(Vec::new()),
            last_snapshot_generation: RwLock::new(HashMap::new()),
            free_space: HashMap::from_iter((0..spu.storage_class_count()).flat_map(|class| {
                (0..spu.disk_count(class)).map(move |disk_id| {
                    let free = spu
                        .effective_free_size(class, disk_id, spu.size_in_blocks(class, disk_id))
                        .as_u64();
                    (
                        DiskOffset::construct_disk_id(class, disk_id),
                        AtomicStorageInfo {
                            free: AtomicU64::new(free),
                            total: AtomicU64::new(free),
                        },
                    )
                })
            })),
            free_space_tier: (0..NUM_STORAGE_CLASSES)
                .map(|_| AtomicStorageInfo::default())
                .collect_vec(),
            allocator: self.allocator,
            allocations: AtomicU64::new(0),
            old_root_allocation: SeqLock::new(None),
            allocators: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_dmu(&self, spu: RootSpu, handler: DbHandler) -> RootDmu {
        let mut strategy: [[Option<u8>; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES] =
            [[None; NUM_STORAGE_CLASSES]; NUM_STORAGE_CLASSES];

        for (dst, src) in strategy.iter_mut().zip(self.alloc_strategy.iter()) {
            assert!(
                src.len() <= NUM_STORAGE_CLASSES,
                "Invalid allocation strategy, can't try more than once per class"
            );

            for (dst, src) in dst.iter_mut().zip(src) {
                *dst = Some(*src);
            }
        }

        Dmu::new(
            self.compression.to_builder(),
            <Checksum as crate::checksum::Checksum>::builder(),
            self.default_storage_class,
            spu,
            strategy,
            ClockCache::new(self.cache_size),
            handler,
            #[cfg(feature = "allocation_log")]
            self.allocation_log_file_path.clone(),
        )
    }

    fn select_root_tree(&self, dmu: Arc<RootDmu>) -> Result<(RootTree<RootDmu>, ObjectPointer)> {
        if let Some(cfg) = &self.metrics {
            metrics_init::<Self>(cfg, dmu.clone())?;
        }

        let root_ptr = if let AccessMode::OpenIfExists | AccessMode::OpenOrCreate = self.access_mode
        {
            match Superblock::<ObjectPointer>::fetch_superblocks(dmu.pool()) {
                Ok(None) if self.access_mode == AccessMode::OpenIfExists => {
                    return Err(Error::InvalidSuperblock)
                }
                Ok(ptr) => ptr,
                Err(e) => return Err(e),
            }
        } else {
            None
        };

        if let Some(sb) = root_ptr {
            let root_ptr = sb.root_ptr;
            let tree = RootTree::open(
                ROOT_DATASET_ID,
                root_ptr,
                DefaultMessageAction,
                dmu,
                ROOT_TREE_STORAGE_PREFERENCE,
            );

            // Update space accounting from last execution
            for (class, info) in sb.tiers.iter().enumerate() {
                let storage_info = tree
                    .dmu()
                    .handler()
                    .free_space_tier
                    .get(class)
                    .expect("Could not fetch valid storage class");
                storage_info
                    .free
                    .store(info.free.as_u64(), Ordering::Release);
                storage_info
                    .total
                    .store(info.total.as_u64(), Ordering::Release);
            }

            *tree.dmu().handler().old_root_allocation.lock_write() =
                Some((root_ptr.offset(), root_ptr.size()));
            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));

            // Iterate over disk space usage
            for (disk_id, space) in tree
                .range(&space_accounting::min_key()[..]..&space_accounting::max_key()[..])?
                .filter_map(|res| res.ok())
            {
                let space_info = tree
                    .dmu()
                    .handler()
                    .free_space
                    .get(&space_accounting::read_key(&disk_id))
                    .unwrap();
                let stored_info: StorageInfo = bincode::deserialize(&space)?;
                space_info
                    .free
                    .store(stored_info.free.as_u64(), Ordering::Relaxed);
                space_info
                    .total
                    .store(stored_info.total.as_u64(), Ordering::Relaxed);
            }

            Ok((tree, root_ptr))
        } else {
            Superblock::<ObjectPointer>::clear_superblock(dmu.pool())?;
            let tree = RootTree::empty_tree(
                ROOT_DATASET_ID,
                DefaultMessageAction,
                dmu,
                ROOT_TREE_STORAGE_PREFERENCE,
            );

            for (tier_id, tier) in tree.dmu().handler().free_space_tier.iter().enumerate() {
                let free =
                    (0..tree.dmu().spl().disk_count(tier_id as u8)).fold(0, |acc, disk_id| {
                        let spu = tree.dmu().spl();
                        acc + spu
                            .effective_free_size(
                                tier_id as u8,
                                disk_id,
                                spu.size_in_blocks(tier_id as u8, disk_id),
                            )
                            .as_u64()
                    });
                tier.free.store(free, Ordering::Relaxed);
                tier.total.store(free, Ordering::Relaxed);
            }

            tree.dmu()
                .handler()
                .root_tree_inner
                .set(Arc::clone(tree.inner()));
            {
                let dmu = tree.dmu();
                for class in 0..dmu.pool().storage_class_count() {
                    for disk_id in 0..dmu.pool().disk_count(class) {
                        dmu.allocate_raw_at(DiskOffset::new(class, disk_id, Block(0)), Block(2))?;
                    }
                }
            }
            let root_ptr = tree.sync()?;
            Ok((tree, root_ptr))
        }
    }

    fn sync_mode(&self) -> SyncMode {
        if let Some(interval_ms) = self.sync_interval_ms {
            SyncMode::Periodic { interval_ms }
        } else {
            SyncMode::Explicit
        }
    }

    fn migration_policy(&self) -> Option<MigrationPolicies> {
        self.migration_policy.clone()
    }
}

type ErasedTree = dyn ErasedTreeSync<Pointer = ObjectPointer, ObjectRef = ObjectRef> + Send + Sync;

/// The database type.
pub struct Database {
    pub(crate) root_tree: RootTree<RootDmu>,
    builder: DatabaseConfiguration,
    open_datasets: HashMap<DatasetId, Box<ErasedTree>>,
    pub(crate) db_tx: Option<Sender<DatabaseMsg>>,
}

impl Database {
    /// Opens a database given by the storage pool configuration.
    pub fn open(cfg: StoragePoolConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::OpenIfExists,
            ..Default::default()
        })
    }

    /// Creates a database given by the storage pool configuration.
    ///
    /// Note that any existing database will be overwritten!
    pub fn create(cfg: StoragePoolConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::AlwaysCreateNew,
            ..Default::default()
        })
    }

    /// Opens or creates a database given by the storage pool configuration.
    pub fn open_or_create(cfg: StoragePoolConfiguration) -> Result<Self> {
        Self::build(DatabaseConfiguration {
            storage: cfg,
            access_mode: AccessMode::OpenOrCreate,
            ..Default::default()
        })
    }

    /// Write the current configuration to the specified path in the json format.
    pub fn write_config_json<P: AsRef<Path>>(&self, p: P) -> Result<()> {
        self.builder.write_to_json(p)
    }

    /// Opens or creates a database given by the storage pool configuration and
    /// sets the given cache size.
    pub fn build(builder: DatabaseConfiguration) -> Result<Self> {
        Self::build_internal(builder, None, None)
    }

    // Construct an instance of [Database] either using external threads or not.
    // Deprecates [with_sync]
    fn build_internal(
        builder: DatabaseConfiguration,
        dml_tx: Option<Sender<DmlMsg>>,
        db_tx: Option<Sender<DatabaseMsg>>,
    ) -> Result<Self> {
        let spl = builder.new_spu()?;
        let handler = builder.new_handler(&spl);
        let mut dmu = builder.new_dmu(spl, handler);
        if let Some(tx) = &dml_tx {
            dmu.set_report(tx.clone());
        }

        #[cfg(feature = "allocation_log")]
        dmu.write_global_header()?;

        let (tree, root_ptr) = builder.select_root_tree(Arc::new(dmu))?;

        *tree.dmu().handler().current_generation.lock_write() = root_ptr.generation().next();
        *tree.dmu().handler().root_tree_snapshot.write() = Some(TreeInner::new_ro(
            RootDmu::root_ref_from_ptr(root_ptr),
            DefaultMessageAction,
        ));

        Ok(Database {
            root_tree: tree,
            builder,
            open_datasets: Default::default(),
            db_tx,
        })
    }

    /// Opens or create a database given by the storage pool configuration, sets the given cache size and spawns threads to periodically perform
    /// sync (if configured with [SyncMode::Periodic]) and auto migration (if configured with [MigrationPolicies]).
    pub fn build_threaded(builder: DatabaseConfiguration) -> Result<Arc<RwLock<Self>>> {
        let db = match builder.migration_policy() {
            Some(pol) => {
                let (dml_tx, dml_rx) = crossbeam_channel::unbounded();
                let (db_tx, db_rx) = crossbeam_channel::unbounded();
                let db = Arc::new(RwLock::new(Self::build_internal(
                    builder,
                    Some(dml_tx),
                    Some(db_tx.clone()),
                )?));

                // Discovery Initializiation
                for os_id in db.read().iter_object_stores()? {
                    // NOTE: If any of the result resolutions here fail the
                    // state of the datastore is anyway corrupt and we can
                    // escalate.
                    let id = os_id?;
                    let os = db.write().open_object_store_with_id(id)?;
                    for (key, info) in os.iter_objects()? {
                        db_tx
                            .send(DatabaseMsg::ObjectDiscover(
                                GlobalObjectId::build(id, info.object_id),
                                info,
                                key,
                            ))
                            .expect("UNREACHABLE");
                    }
                    db.write().close_object_store(os);
                }

                let other = db.clone();
                thread::spawn(move || {
                    let hints = other.read().root_tree.dmu().storage_hints();
                    let mut policy = pol.construct(dml_rx, db_rx, other, hints);
                    loop {
                        if let Err(e) = policy.thread_loop() {
                            error!("Automatic Migration Policy encountered {:?}", e);
                            error!("Continuing and reinitializing policy to avoid errors, but functionality may be limited.");
                        }
                    }
                });
                db
            }
            None => Arc::new(RwLock::new(Self::build_internal(builder, None, None)?)),
        };
        Ok(Self::with_sync(db))
    }

    /// If this [Database] was created with a [SyncMode::Periodic], this function
    /// will wrap self in an `Arc<RwLock<_>>` and start a thread to periodically
    /// call `self.sync()`.
    ///
    /// This is a separate step from [Database::build] because some usecases don't require
    /// periodic syncing.
    fn with_sync(this: Arc<RwLock<Self>>) -> Arc<RwLock<Self>> {
        if let SyncMode::Periodic { interval_ms } = this.read().builder.sync_mode() {
            thread::spawn({
                let db = this.clone();
                move || sync_timer::sync_timer(interval_ms, db)
            });
        }
        this
    }

    fn sync_ds(&self, ds_id: DatasetId, ds_tree: &ErasedTree) -> Result<()> {
        trace!("sync_ds: Enter");
        let ptr = ds_tree.erased_sync()?;
        trace!("sync_ds: erased_sync");
        let msg = DatasetData::update_ptr(ptr)?;
        let key = &dataset_key::data_key(ds_id) as &[_];
        self.root_tree.insert(key, msg, StoragePreference::NONE)?;
        Ok(())
    }

    fn flush_delayed_messages(&self) -> Result<()> {
        loop {
            let v = std::mem::take(&mut *self.root_tree.dmu().handler().delayed_messages.lock());
            if v.is_empty() {
                break;
            }
            for (key, msg) in v {
                self.root_tree.insert(key, msg, StoragePreference::NONE)?;
            }
        }
        Ok(())
    }

    /// Synchronizes the database.
    pub fn sync(&mut self) -> Result<()> {
        let mut ds_locks = Vec::with_capacity(self.open_datasets.len());
        for (&ds_id, ds_tree) in &self.open_datasets {
            loop {
                if let Some(lock) = ds_tree.erased_try_lock_root() {
                    ds_locks.push(lock);
                    break;
                }
                info!("Sync: syncing tree of {:?}", ds_id);
                self.sync_ds(ds_id, ds_tree.as_ref())?;
            }
        }
        let root_ptr = loop {
            self.flush_delayed_messages()?;
            let allocations_before = self
                .root_tree
                .dmu()
                .handler()
                .allocations
                .load(Ordering::Acquire);
            info!("Sync: syncing root tree");

            let root_ptr = self.root_tree.sync()?;
            let allocations_after = self
                .root_tree
                .dmu()
                .handler()
                .allocations
                .load(Ordering::Acquire);
            let allocations = allocations_after - allocations_before;
            if allocations <= 1 {
                break root_ptr;
            } else {
                info!("Sync: resyncing -- seen {} allocations", allocations);
            }
        };
        let pool = self.root_tree.dmu().spl();
        pool.flush()?;
        let mut info = [StorageInfo {
            free: Block(0),
            total: Block(0),
        }; NUM_STORAGE_CLASSES];
        for (idx, info_val) in info.iter_mut().enumerate().take(NUM_STORAGE_CLASSES) {
            *info_val = self
                .root_tree
                .dmu()
                .handler()
                .free_space_tier(idx as u8)
                .expect("Class hat to exist");
        }
        Superblock::<ObjectPointer>::write_superblock(pool, &root_ptr, &info)?;
        pool.flush()?;
        let handler = self.root_tree.dmu().handler();
        *handler.old_root_allocation.lock_write() = Some((root_ptr.offset(), root_ptr.size()));
        handler.bump_generation();
        handler
            .root_tree_snapshot
            .write()
            .as_mut()
            .unwrap()
            .update_root_node(RootDmu::root_ref_from_ptr(root_ptr));
        Ok(())
    }

    /// Drops the entire cache. This is useful when considering performance
    /// measurements regarding "cold" environments.
    pub fn drop_cache(&self) -> Result<()> {
        self.root_tree.dmu().drop_cache();
        Ok(())
    }

    /// Storage tier information for all available tiers. These are in order as in `storage_prefernce.as_u8()`
    pub fn free_space_tier(&self) -> Vec<StorageInfo> {
        (0..self.root_tree.dmu().spl().storage_class_count())
            .map(|class| {
                self.root_tree
                    .dmu()
                    .handler()
                    .free_space_tier(class)
                    .unwrap()
            })
            .collect()
    }

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn root_tree(&self) -> &RootTree<RootDmu> {
        &self.root_tree
    }
}

fn fetch_ds_data<T>(root_tree: &T, id: DatasetId) -> Result<DatasetData<ObjectPointer>>
where
    T: TreeLayer<DefaultMessageAction>,
{
    let key = &dataset_key::data_key(id) as &[_];
    let data = root_tree.get(key)?.ok_or(Error::DoesNotExist)?;
    DatasetData::unpack(&data)
}

fn fetch_ss_data<T>(
    root_tree: &T,
    ds_id: DatasetId,
    ss_id: Generation,
) -> Result<DatasetData<ObjectPointer>>
where
    T: TreeLayer<DefaultMessageAction>,
{
    let key = snapshot_key::data_key(ds_id, ss_id);
    let data = root_tree.get(key)?.ok_or(Error::DoesNotExist)?;
    DatasetData::unpack(&data)
}

#[derive(Debug, Serialize, Deserialize)]
struct DeadListData {
    size: Block<u32>,
    birth: Generation,
}

impl DeadListData {
    fn pack(&self) -> Result<[u8; 12]> {
        let mut buf = [0; 12];
        serialize_into(&mut buf[..], self)?;
        Ok(buf)
    }
    fn unpack(b: &[u8]) -> Result<Self> {
        Ok(deserialize(b)?)
    }
}

/// Internal identifier for a dataset
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct DatasetId(u64);

use std::fmt::Display;

impl Display for DatasetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
    }
}

impl DatasetId {
    pub(crate) fn pack(self) -> [u8; 8] {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, self.0);
        b
    }

    pub(crate) fn unpack(b: &[u8]) -> Self {
        DatasetId(BigEndian::read_u64(b))
    }

    pub(crate) fn next(self) -> Self {
        DatasetId(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl StaticSize for DatasetId {
    fn static_size() -> usize {
        8
    }
}

#[derive(Debug)]
struct DatasetData<P> {
    previous_snapshot: Option<Generation>,
    ptr: P,
}

impl<P> DatasetData<P> {
    fn update_previous_snapshot(x: Option<Generation>) -> SlicedCowBytes {
        let mut b = [0; 8];
        let x = if let Some(generation) = x {
            assert_ne!(generation.0, 0);
            generation.0
        } else {
            0
        };
        LittleEndian::write_u64(&mut b[..], x);

        DefaultMessageAction::upsert_msg(0, &b)
    }
}

impl<P: Serialize> DatasetData<P> {
    fn update_ptr(ptr: P) -> Result<SlicedCowBytes> {
        let mut v = Vec::new();
        serialize_into(&mut v, &ptr)?;
        let msg = DefaultMessageAction::upsert_msg(8, &v);
        Ok(msg)
    }

    fn pack(&self) -> Result<Vec<u8>> {
        let mut v = vec![0; 8];
        let x = if let Some(generation) = self.previous_snapshot {
            assert_ne!(generation.0, 0);
            generation.0
        } else {
            0
        };
        LittleEndian::write_u64(&mut v, x);
        serialize_into(&mut v, &self.ptr)?;
        Ok(v)
    }
}
impl<P: DeserializeOwned> DatasetData<P> {
    fn unpack(b: &[u8]) -> Result<Self> {
        let x = LittleEndian::read_u64(
            b.get(..8)
                .ok_or(Error::Generic("invalid data".to_string()))?,
        );
        let ptr = deserialize(&b[8..])?;
        Ok(DatasetData {
            previous_snapshot: if x > 0 { Some(Generation(x)) } else { None },
            ptr,
        })
    }
}

/// Internal identifier of a generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Generation(u64);

impl StaticSize for Generation {
    fn static_size() -> usize {
        8
    }
}

impl Generation {
    fn pack(self) -> [u8; 8] {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, self.0);
        b
    }

    fn unpack(b: &[u8]) -> Self {
        Generation(BigEndian::read_u64(b))
    }

    fn next(self) -> Self {
        Generation(self.0 + 1)
    }
}
