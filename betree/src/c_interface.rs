//! This module provides the C interface to the database.
#![allow(non_camel_case_types)]
use std::{
    env::SplitPaths,
    ffi::{CStr, OsStr},
    io::{stderr, BufReader, Write},
    ops::Deref,
    os::{
        raw::{c_char, c_int, c_uint, c_ulong},
        unix::prelude::OsStrExt,
    },
    process::abort,
    ptr::{null_mut, read, write},
    slice::{from_raw_parts, from_raw_parts_mut},
    sync::Arc,
};

use libc::{c_void, memcpy};
use parking_lot::RwLock;

use crate::{
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::Dml,
    database::{AccessMode, Database, Dataset, Error, Snapshot},
    object::{ObjectHandle, ObjectStore},
    storage_pool::{LeafVdev, StoragePoolConfiguration, TierConfiguration, Vdev},
    tree::DefaultMessageAction,
    DatabaseConfiguration, StoragePreference,
};

/// The type for a database configuration
pub struct cfg_t(DatabaseConfiguration);
/// The general error type
pub struct err_t(Error);
/// The storage preference
#[repr(C)]
pub struct storage_pref_t(StoragePreference);
/// The database type
pub struct db_t(Database);
/// The data set type
pub struct ds_t(Dataset);
/// The snapshot type
pub struct ss_t(Snapshot);
/// The data set/snapshot name iterator type
pub struct name_iter_t(Box<dyn Iterator<Item = Result<SlicedCowBytes, Error>>>);
/// The range iterator type
pub struct range_iter_t(Box<dyn Iterator<Item = Result<(CowBytes, SlicedCowBytes), Error>>>);

/// The object store wrapper type
pub struct obj_store_t(ObjectStore);
/// The handle of an object in the corresponding object store
pub struct obj_t<'os>(ObjectHandle<'os>);

pub const STORAGE_PREF_NONE: storage_pref_t = storage_pref_t(StoragePreference::NONE);
pub const STORAGE_PREF_FASTEST: storage_pref_t = storage_pref_t(StoragePreference::FASTEST);
pub const STORAGE_PREF_SLOWEST: storage_pref_t = storage_pref_t(StoragePreference::SLOWEST);

/// A reference counted byte slice
#[repr(C)]
pub struct byte_slice_t {
    ptr: *const c_char,
    len: c_uint,
    arc: *const byte_slice_rc_t,
}

impl From<CowBytes> for byte_slice_t {
    fn from(x: CowBytes) -> Self {
        let ptr = &x[..] as *const [u8] as *const u8 as *const c_char;
        let len = x.len() as c_uint;
        let arc = Arc::into_raw(x.inner) as *const byte_slice_rc_t;
        byte_slice_t { ptr, len, arc }
    }
}

impl From<SlicedCowBytes> for byte_slice_t {
    fn from(x: SlicedCowBytes) -> Self {
        let ptr = &x[..] as *const [u8] as *const u8 as *const c_char;
        let len = x.len() as c_uint;
        let arc = Arc::into_raw(x.data.inner) as *const byte_slice_rc_t;
        byte_slice_t { ptr, len, arc }
    }
}

impl Drop for byte_slice_t {
    fn drop(&mut self) {
        unsafe { Arc::from_raw(self.arc as *const Vec<u8>) };
    }
}

/// A byte slice reference counter
// Intentionally not #[repr(C)], or cbindgen will expose Vec internals
struct byte_slice_rc_t(Vec<u8>);

trait HandleResult {
    type Result;
    fn success(self) -> Self::Result;
    fn fail() -> Self::Result;
}

impl HandleResult for () {
    type Result = c_int;
    fn success(self) -> c_int {
        0
    }
    fn fail() -> c_int {
        -1
    }
}

impl HandleResult for DatabaseConfiguration {
    type Result = *mut cfg_t;
    fn success(self) -> *mut cfg_t {
        b(cfg_t(self))
    }
    fn fail() -> *mut cfg_t {
        null_mut()
    }
}

impl HandleResult for Database {
    type Result = *mut db_t;
    fn success(self) -> *mut db_t {
        b(db_t(self))
    }
    fn fail() -> *mut db_t {
        null_mut()
    }
}

impl HandleResult for Arc<RwLock<Database>> {
    type Result = *mut db_t;
    fn success(self) -> *mut db_t {
        unsafe {
            let rwlock_db = Arc::into_raw(self);
            let db = rwlock_db.read();
            b(db_t(db.into_inner()))
        }
    }
    fn fail() -> *mut db_t {
        null_mut()
    }
}

impl HandleResult for Dataset {
    type Result = *mut ds_t;
    fn success(self) -> *mut ds_t {
        b(ds_t(self))
    }
    fn fail() -> *mut ds_t {
        null_mut()
    }
}

impl HandleResult for Snapshot {
    type Result = *mut ss_t;
    fn success(self) -> *mut ss_t {
        b(ss_t(self))
    }
    fn fail() -> *mut ss_t {
        null_mut()
    }
}

impl HandleResult for Box<dyn Iterator<Item = Result<SlicedCowBytes, Error>>> {
    type Result = *mut name_iter_t;
    fn success(self) -> *mut name_iter_t {
        b(name_iter_t(self))
    }
    fn fail() -> *mut name_iter_t {
        null_mut()
    }
}

impl HandleResult for Box<dyn Iterator<Item = Result<(CowBytes, SlicedCowBytes), Error>>> {
    type Result = *mut range_iter_t;
    fn success(self) -> *mut range_iter_t {
        b(range_iter_t(self))
    }
    fn fail() -> *mut range_iter_t {
        null_mut()
    }
}

impl HandleResult for ObjectStore {
    type Result = *mut obj_store_t;
    fn success(self) -> *mut obj_store_t {
        b(obj_store_t(self))
    }
    fn fail() -> *mut obj_store_t {
        null_mut()
    }
}

impl<'os> HandleResult for ObjectHandle<'os> {
    type Result = *mut obj_t<'os>;
    fn success(self) -> *mut obj_t<'os> {
        b(obj_t(self))
    }
    fn fail() -> *mut obj_t<'os> {
        null_mut()
    }
}

trait HandleResultExt {
    type Result;
    fn handle_result(self, err: *mut *mut err_t) -> Self::Result;
}

impl<T: HandleResult> HandleResultExt for Result<T, Error> {
    type Result = T::Result;
    fn handle_result(self, err: *mut *mut err_t) -> T::Result {
        match self {
            Ok(x) => x.success(),
            Err(e) => {
                handle_err(e, err);
                T::fail()
            }
        }
    }
}

impl<T: HandleResult> HandleResultExt for Result<Option<T>, Error> {
    type Result = T::Result;
    fn handle_result(self, err: *mut *mut err_t) -> T::Result {
        match self {
            Ok(Some(x)) => x.success(),
            Ok(None) => T::fail(),
            Err(e) => {
                handle_err(e, err);
                T::fail()
            }
        }
    }
}

fn handle_err(e: Error, ptr: *mut *mut err_t) {
    if !ptr.is_null() {
        let r = unsafe { &mut *ptr };
        *r = Box::into_raw(Box::new(err_t(e)));
    }
}

fn b<T>(x: T) -> *mut T {
    Box::into_raw(Box::new(x))
}

/// Parse the configuration string for a storage pool.
///
/// On success, return a `cfg_t` which has to be freed with `betree_free_cfg`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_parse_configuration(
    cfg_strings: *const *const c_char,
    cfg_strings_len: c_uint,
    err: *mut *mut err_t,
) -> *mut cfg_t {
    let cfg_strings = from_raw_parts(cfg_strings, cfg_strings_len as usize);
    let cfg_string_iter = cfg_strings
        .iter()
        .map(|&p| CStr::from_ptr(p).to_string_lossy());

    TierConfiguration::parse_zfs_like(cfg_string_iter)
        .map(|tier_cfg| DatabaseConfiguration {
            storage: StoragePoolConfiguration {
                tiers: vec![tier_cfg],
                ..Default::default()
            },
            ..Default::default()
        })
        .map_err(Error::from)
        .handle_result(err)
}

/// Parse configuration from file specified in environment (BETREE_CONFIG).
///
/// On success, return a `cfg_t` which has to be freed with `betree_free_cfg`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_configuration_from_env(err: *mut *mut err_t) -> *mut cfg_t {
    let path = match std::env::var_os("BETREE_CONFIG") {
        Some(val) => val,
        None => {
            handle_err(
                Error::Generic("Environment variable BETREE_CONFIG is not defined.".into()),
                err,
            );
            return null_mut();
        }
    };
    let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    serde_json::from_reader::<_, DatabaseConfiguration>(BufReader::new(file))
        .map_err(Error::from)
        .handle_result(err)
}

/// Enable the global env_logger, configured via environment variables.
#[cfg(feature = "init_env_logger")]
#[no_mangle]
pub unsafe extern "C" fn betree_init_env_logger() {
    crate::env_logger::init_env_logger()
}

/// Free a configuration object.
#[no_mangle]
pub unsafe extern "C" fn betree_free_cfg(cfg: *mut cfg_t) {
    let _ = Box::from_raw(cfg);
}

/// Free an error object.
#[no_mangle]
pub unsafe extern "C" fn betree_free_err(err: *mut err_t) {
    let _ = Box::from_raw(err);
}

/// Free a byte slice.
#[no_mangle]
pub unsafe extern "C" fn betree_free_byte_slice(x: *mut byte_slice_t) {
    read(x);
}

/// Free a data set/snapshot name iterator.
#[no_mangle]
pub unsafe extern "C" fn betree_free_name_iter(name_iter: *mut name_iter_t) {
    let _ = Box::from_raw(name_iter);
}

/// Free a range iterator.
#[no_mangle]
pub unsafe extern "C" fn betree_free_range_iter(range_iter: *mut range_iter_t) {
    let _ = Box::from_raw(range_iter);
}

fn set_leaf_vdev_direct(leaf: &mut LeafVdev) {
    if let LeafVdev::FileWithOpts { direct, .. } = leaf {
        *direct = Some(true)
    }
}

/// Resets the access modes for all applicable vdevs to 'Direct'.
#[no_mangle]
pub unsafe extern "C" fn betree_configuration_set_direct(cfg: *mut cfg_t, direct: i32) {
    if direct == 1 {
        for tier in (*cfg).0.storage.tiers.iter_mut() {
            for vdev in tier.top_level_vdevs.iter_mut() {
                match vdev {
                    crate::storage_pool::Vdev::Leaf(ref mut l) => set_leaf_vdev_direct(l),
                    crate::storage_pool::Vdev::Mirror { ref mut mirror } => {
                        for l in mirror.iter_mut() {
                            set_leaf_vdev_direct(l)
                        }
                    }
                    crate::storage_pool::Vdev::Parity1 { ref mut parity1 } => {
                        for l in parity1.iter_mut() {
                            set_leaf_vdev_direct(l)
                        }
                    }
                }
            }
        }
    }
}

/// Sets the storage pools disk-dependent iodepth.
#[no_mangle]
pub unsafe extern "C" fn betree_configuration_set_iodepth(cfg: *mut cfg_t, iodepth: u32) {
    (*cfg).0.storage.queue_depth_factor = iodepth
}

/// Reconfigures the given configuration to use a single tier with the given path as the sole backing disk.
#[no_mangle]
pub unsafe extern "C" fn betree_configuration_set_disks(
    cfg: *mut cfg_t,
    paths: *const *const c_char,
    num_disks: usize,
) {
    let path_slice = from_raw_parts(paths, num_disks);
    let disks: Vec<Vdev> = (0..num_disks)
        .map(|pos| {
            let p = CStr::from_ptr(path_slice[pos]);
            Vdev::Leaf(LeafVdev::FileWithOpts {
                path: p.to_str().unwrap().into(),
                direct: Some(false),
            })
        })
        .collect();
    (*cfg).0.storage.tiers = vec![TierConfiguration {
        top_level_vdevs: disks,
        ..Default::default()
    }]
}

/// Build a database given by a configuration.
///
/// On success, return a `db_t` which has to be freed with `betree_close_db`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_build_db(cfg: *const cfg_t, err: *mut *mut err_t) -> *mut db_t {
    Database::build_threaded((*cfg).0.clone()).handle_result(err)
}

/// Open a database given by a configuration. If no initialized database is present this procedure will fail.
///
/// On success, return a `db_t` which has to be freed with `betree_close_db`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_open_db(cfg: *const cfg_t, err: *mut *mut err_t) -> *mut db_t {
    let mut db_cfg = (*cfg).0.clone();
    db_cfg.access_mode = AccessMode::OpenIfExists;
    Database::build_threaded(db_cfg).handle_result(err)
}

/// Create a database given by a configuration.
///
/// On success, return a `db_t` which has to be freed with `betree_close_db`.
/// On error, return null.  If `err` is not null, store an error in `err`.
///
/// Note that any existing database will be overwritten!
#[no_mangle]
pub unsafe extern "C" fn betree_create_db(cfg: *const cfg_t, err: *mut *mut err_t) -> *mut db_t {
    let mut db_cfg = (*cfg).0.clone();
    db_cfg.access_mode = AccessMode::AlwaysCreateNew;
    Database::build_threaded(db_cfg).handle_result(err)
}

/// Create a database given by a configuration.
///
/// On success, return a `db_t` which has to be freed with `betree_close_db`.
/// On error, return null.  If `err` is not null, store an error in `err`.
///
/// Note that any existing database will be overwritten!
#[no_mangle]
pub unsafe extern "C" fn betree_open_or_create_db(
    cfg: *const cfg_t,
    err: *mut *mut err_t,
) -> *mut db_t {
    let mut db_cfg = (*cfg).0.clone();
    db_cfg.access_mode = AccessMode::OpenOrCreate;
    // BUG: This returns a nullptr
    Database::build_threaded(db_cfg).handle_result(err)
}

/// Sync a database.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_sync_db(db: *mut db_t, err: *mut *mut err_t) -> c_int {
    let db = &mut (*db).0;
    db.sync().handle_result(err)
}

/// Closes a database.
///
/// Note that the `db_t` may not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_close_db(db: *mut db_t) {
    let _ = Box::from_raw(db);
}

/// Open a data set identified by the given name.
///
/// On success, return a `ds_t` which has to be freed with `betree_close_ds`.
/// On error, return null.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_open_ds(
    db: *mut db_t,
    name: *const c_char,
    len: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> *mut ds_t {
    let db = &mut (*db).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.open_custom_dataset::<DefaultMessageAction>(name, storage_pref.0)
        .handle_result(err)
}

/// Create a new data set with the given name.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the creation fails if a data set with same name exists already.
#[no_mangle]
pub unsafe extern "C" fn betree_create_ds(
    db: *mut db_t,
    name: *const c_char,
    len: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.create_custom_dataset::<DefaultMessageAction>(name, storage_pref.0)
        .handle_result(err)
}

/// Close a data set.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the `ds_t` may not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_close_ds(
    db: *mut db_t,
    ds: *mut ds_t,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let ds = Box::from_raw(ds).0;
    db.close_dataset(ds).handle_result(err)
}

/// Iterate over all data sets of a database.
///
/// On success, return a `name_iter_t` which has to be freed with
/// `betree_free_name_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_iter_datasets(
    db: *mut db_t,
    err: *mut *mut err_t,
) -> *mut name_iter_t {
    let db = &mut (*db).0;
    db.iter_datasets()
        .map(|it| Box::new(it) as Box<dyn Iterator<Item = Result<SlicedCowBytes, Error>>>)
        .handle_result(err)
}

/// Save the next item in the iterator in `name`.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that `name` may not be used on error but on success,
/// it has to be freed with `betree_free_byte_slice` afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_name_iter_next(
    name_iter: *mut name_iter_t,
    name: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let name_iter = &mut (*name_iter).0;
    match name_iter.next() {
        None => -1,
        Some(Err(e)) => {
            handle_err(e, err);
            -1
        }
        Some(Ok(next_name)) => {
            write(name, next_name.into());
            0
        }
    }
}

/// Save the next key-value pair in the iterator.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that `key` and `value` may not be used on error but on success,
/// both have to be freed with `betree_free_byte_slice` afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_range_iter_next(
    range_iter: *mut range_iter_t,
    key: *mut byte_slice_t,
    value: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let range_iter = &mut (*range_iter).0;
    match range_iter.next() {
        None => -1,
        Some(Err(e)) => {
            handle_err(e, err);
            -1
        }
        Some(Ok((next_key, next_value))) => {
            write(key, next_key.into());
            write(value, next_value.into());
            0
        }
    }
}

/// Create a new snapshot for the given data set with the given name.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the creation fails if a snapshot with same name exists already
/// for this data set.
#[no_mangle]
pub unsafe extern "C" fn betree_create_snapshot(
    db: *mut db_t,
    ds: *mut ds_t,
    name: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let ds = &mut (*ds).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.create_snapshot(ds, name).handle_result(err)
}

/// Delete the snapshot for the given data set with the given name.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the deletion fails if a snapshot with the given name does not
/// exist for this data set.
#[no_mangle]
pub unsafe extern "C" fn betree_delete_snapshot(
    db: *mut db_t,
    ds: *mut ds_t,
    name: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let db = &mut (*db).0;
    let ds = &mut (*ds).0;
    let name = from_raw_parts(name as *const u8, len as usize);
    db.delete_snapshot(ds, name).handle_result(err)
}

/// Iterate over all snapshots of a data set.
///
/// On success, return a `name_iter_t` which has to be freed with
/// `betree_free_name_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_iter_snapshots(
    db: *const db_t,
    ds: *const ds_t,
    err: *mut *mut err_t,
) -> *mut name_iter_t {
    let db = &(*db).0;
    let ds = &(*ds).0;
    db.iter_snapshots(ds)
        .map(|it| Box::new(it) as Box<dyn Iterator<Item = Result<SlicedCowBytes, Error>>>)
        .handle_result(err)
}

/// Retrieve the `value` for the given `key`.
///
/// On success, return 0.  If the key does not exist, return -1.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that on success `value` has to be freed with `betree_free_byte_slice`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_get(
    ds: *const ds_t,
    key: *const c_char,
    len: c_uint,
    value: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, len as usize);
    match ds.get(key) {
        Err(e) => {
            handle_err(e, err);
            -1
        }
        Ok(None) => -1,
        Ok(Some(v)) => {
            write(value, v.into());
            0
        }
    }
}

/// Delete the value for the given `key` if the key exists.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_delete(
    ds: *const ds_t,
    key: *const c_char,
    len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, len as usize);
    ds.delete(key).handle_result(err)
}

/// Insert the given key-value pair.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that any existing value will be overwritten.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_insert(
    ds: *const ds_t,
    key: *const c_char,
    key_len: c_uint,
    data: *const c_char,
    data_len: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, key_len as usize);
    let data = from_raw_parts(data as *const u8, data_len as usize);
    ds.insert_with_pref(key, data, storage_pref.0)
        .handle_result(err)
}

/// Upsert the value for the given key at the given offset.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that the value will be zeropadded as needed.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_upsert(
    ds: *const ds_t,
    key: *const c_char,
    key_len: c_uint,
    data: *const c_char,
    data_len: c_uint,
    offset: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let key = from_raw_parts(key as *const u8, key_len as usize);
    let data = from_raw_parts(data as *const u8, data_len as usize);
    ds.upsert_with_pref(key, data, offset, storage_pref.0)
        .handle_result(err)
}

/// Delete all key-value pairs in the given key range.
/// `low_key` is inclusive, `high_key` is exclusive.
///
/// On success, return 0.
/// On error, return -1.  If `err` is not null, store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_range_delete(
    ds: *const ds_t,
    low_key: *const c_char,
    low_key_len: c_uint,
    high_key: *const c_char,
    high_key_len: c_uint,
    err: *mut *mut err_t,
) -> c_int {
    let ds = &(*ds).0;
    let low_key = from_raw_parts(low_key as *const u8, low_key_len as usize);
    let high_key = from_raw_parts(high_key as *const u8, high_key_len as usize);
    ds.range_delete(low_key..high_key).handle_result(err)
}

/// Return the data set's name.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_name(ds: *const ds_t, len: *mut c_uint) -> *const c_char {
    let ds = &(*ds).0;
    let name = ds.name();
    let ret = &name[..] as *const [u8] as *const u8 as *const c_char;
    write(len, name.len() as c_uint);
    ret
}

/// Iterate over all key-value pairs in the given key range.
/// `low_key` is inclusive, `high_key` is exclusive.
///
/// On success, return a `range_iter_t` which has to be freed with
/// `betree_free_range_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_dataset_range(
    ds: *const ds_t,
    low_key: *const c_char,
    low_key_len: c_uint,
    high_key: *const c_char,
    high_key_len: c_uint,
    err: *mut *mut err_t,
) -> *mut range_iter_t {
    let ds = &(*ds).0;
    let low_key = from_raw_parts(low_key as *const u8, low_key_len as usize);
    let high_key = from_raw_parts(high_key as *const u8, high_key_len as usize);
    ds.range(low_key..high_key).handle_result(err)
}

/// Retrieve the `value` for the given `key`.
///
/// On success, return 0.  If the key does not exist, return -1.
/// On error, return -1.  If `err` is not null, store an error in `err`.
///
/// Note that on success `value` has to be freed with `betree_free_byte_slice`.
#[no_mangle]
pub unsafe extern "C" fn betree_snapshot_get(
    ss: *const ss_t,
    key: *const c_char,
    len: c_uint,
    value: *mut byte_slice_t,
    err: *mut *mut err_t,
) -> c_int {
    let ss = &(*ss).0;
    let key = from_raw_parts(key as *const u8, len as usize);
    match ss.get(key) {
        Err(e) => {
            handle_err(e, err);
            -1
        }
        Ok(None) => -1,
        Ok(Some(v)) => {
            write(value, v.into());
            0
        }
    }
}

/// Iterate over all key-value pairs in the given key range.
/// `low_key` is inclusive, `high_key` is exclusive.
///
/// On success, return a `range_iter_t` which has to be freed with
/// `betree_free_range_iter`. On error, return null.  If `err` is not null,
/// store an error in `err`.
#[no_mangle]
pub unsafe extern "C" fn betree_snapshot_range(
    ss: *const ss_t,
    low_key: *const c_char,
    low_key_len: c_uint,
    high_key: *const c_char,
    high_key_len: c_uint,
    err: *mut *mut err_t,
) -> *mut range_iter_t {
    let ss = &(*ss).0;
    let low_key = from_raw_parts(low_key as *const u8, low_key_len as usize);
    let high_key = from_raw_parts(high_key as *const u8, high_key_len as usize);
    ss.range(low_key..high_key).handle_result(err)
}

/// Print the given error to stderr.
#[no_mangle]
pub unsafe extern "C" fn betree_print_error(err: *mut err_t) {
    let err = &(*err).0;
    if write!(&mut stderr(), "{}", err).is_err() {
        abort();
    }
}

/// Create an object store interface.
#[no_mangle]
pub unsafe extern "C" fn betree_create_object_store(
    db: *mut db_t,
    name: *const c_char,
    name_len: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> *mut obj_store_t {
    let db = &mut (*db).0;
    let name = from_raw_parts(name as *const u8, name_len as usize);

    db.open_named_object_store(name, storage_pref.0)
        .handle_result(err)
}

/// Open an existing object.
#[no_mangle]
pub unsafe extern "C" fn betree_object_open<'os>(
    os: *mut obj_store_t,
    key: *const c_char,
    key_len: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> *mut obj_t<'os> {
    let os = &mut (*os).0;
    os.open_object_with_pref(
        from_raw_parts(key as *const u8, key_len as usize),
        storage_pref.0,
    )
    .map(|res| res.map(|(obj, _info)| obj))
    .handle_result(err)
}

/// Create a new object.
#[no_mangle]
pub unsafe extern "C" fn betree_object_create<'os>(
    os: *mut obj_store_t,
    key: *const c_char,
    key_len: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> *mut obj_t<'os> {
    let os = &mut (*os).0;
    os.create_object_with_pref(
        from_raw_parts(key as *const u8, key_len as usize),
        storage_pref.0,
    )
    .map(|(obj, _info)| obj)
    .handle_result(err)
}

/// Try to open an existing object, create it if none exists.
#[no_mangle]
pub unsafe extern "C" fn betree_object_open_or_create<'os>(
    os: *mut obj_store_t,
    key: *const c_char,
    key_len: c_uint,
    storage_pref: storage_pref_t,
    err: *mut *mut err_t,
) -> *mut obj_t<'os> {
    let os = &mut (*os).0;
    os.open_or_create_object_with_pref(
        from_raw_parts(key as *const u8, key_len as usize),
        storage_pref.0,
    )
    .map(|(obj, _info)| obj)
    .handle_result(err)
}

/// Delete an existing object. The handle may not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_object_delete(obj: *mut obj_t, err: *mut *mut err_t) -> c_int {
    let obj = Box::from_raw(obj).0;
    obj.delete().handle_result(err)
}

/// Closes an object. The handle may not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn betree_object_close(obj: *mut obj_t, err: *mut *mut err_t) -> c_int {
    let obj = Box::from_raw(obj).0;
    obj.close().handle_result(err)
}

/// Try to read `buf_len` bytes of `obj` into `buf`, starting at `offset` bytes into the objects
/// data. The actually read number of bytes is written into `n_read` if and only if the read
/// succeeded.
#[no_mangle]
pub unsafe extern "C" fn betree_object_read_at(
    obj: *mut obj_t,
    buf: *mut c_char,
    buf_len: c_ulong,
    offset: c_ulong,
    n_read: *mut c_ulong,
    err: *mut *mut err_t,
) -> c_int {
    let obj = &mut (*obj).0;
    let buf = from_raw_parts_mut(buf as *mut u8, buf_len as usize);
    obj.read_at(buf, offset)
        .map(|read| {
            *n_read = read as c_ulong;
        })
        .map_err(|(read, err)| {
            *n_read = read;
            err
        })
        .handle_result(err)
}

/// Try to write `buf_len` bytes from `buf` into `obj`, starting at `offset` bytes into the objects
/// data.
#[no_mangle]
pub unsafe extern "C" fn betree_object_write_at(
    obj: *mut obj_t,
    buf: *const c_char,
    buf_len: c_ulong,
    offset: c_ulong,
    n_written: *mut c_ulong,
    err: *mut *mut err_t,
) -> c_int {
    let obj = &mut (*obj).0;
    let buf = from_raw_parts(buf as *const u8, buf_len as usize);
    obj.write_at(buf, offset)
        .map(|written| {
            *n_written = written;
        })
        .map_err(|(written, err)| {
            *n_written = written;
            err
        })
        .handle_result(err)
}

/*
/// Return the objects size in bytes.
#[no_mangle]
pub unsafe extern "C" fn betree_object_status(obj: *const obj_t, err: *mut *mut err_t) -> c_ulong {
    let obj = &(*obj).0;
    let info = obj.info();
    obj.
    obj.size()
}

/// Returns the last modification timestamp in microseconds since the Unix epoch.
#[no_mangle]
pub unsafe extern "C" fn betree_object_mtime_us(obj: *const obj_t) -> c_ulong {
    let obj = &(*obj).0;
    obj.modification_time()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}
*/
