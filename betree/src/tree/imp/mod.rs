use self::{flush::Ref, node::GetResult};
use super::{
    errors::*,
    layer::{ErasedTreeSync, TreeBaseLayer, TreeLayer},
};
use crate::{
    cache::AddSize,
    cow_bytes::{CowBytes, SlicedCowBytes},
    data_management::{
        impls::ObjectPointer, Dml, DmlBase, HandlerDml, HasStoragePreference, ObjectRef,
    },
    range_validation::is_inclusive_non_empty,
    size::StaticSize,
    tree::MessageAction,
    StoragePreference,
};
use owning_ref::OwningRef;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::{
    borrow::Borrow,
    collections::VecDeque,
    marker::PhantomData,
    mem,
    ops::{Deref, RangeBounds},
};

#[derive(Debug)]
enum FillUpResult {
    Rebalanced(CowBytes),
    Merged,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyInfo {
    storage_preference: StoragePreference,
}

impl StaticSize for KeyInfo {
    fn static_size() -> usize {
        mem::size_of::<StoragePreference>()
    }
}

impl KeyInfo {
    pub(crate) fn merge_with_upper(self, upper: KeyInfo) -> KeyInfo {
        KeyInfo {
            storage_preference: StoragePreference::choose_faster(
                self.storage_preference,
                upper.storage_preference,
            ),
        }
    }

    pub(crate) fn storage_preference(&self) -> &StoragePreference {
        &self.storage_preference
    }
}

pub(super) const MAX_INTERNAL_NODE_SIZE: usize = 4 * 1024 * 1024;
const MIN_FLUSH_SIZE: usize = 256 * 1024;
const MIN_FANOUT: usize = 4;
const MIN_LEAF_NODE_SIZE: usize = 1 * 1024 * 1024;
const MAX_LEAF_NODE_SIZE: usize = MAX_INTERNAL_NODE_SIZE;
pub(crate) const MAX_MESSAGE_SIZE: usize = 512 * 1024;

/// The actual tree type.
pub struct Tree<X: DmlBase, M, I: Borrow<Inner<X::ObjectRef, X::Info, M>>> {
    inner: I,
    dml: X,
    evict: bool,
    marker: PhantomData<M>,
    storage_preference: StoragePreference,
}

impl<X: Clone + DmlBase, M, I: Clone + Borrow<Inner<X::ObjectRef, X::Info, M>>> Clone
    for Tree<X, M, I>
{
    fn clone(&self) -> Self {
        Tree {
            inner: self.inner.clone(),
            dml: self.dml.clone(),
            evict: self.evict,
            marker: PhantomData,
            storage_preference: self.storage_preference,
        }
    }
}

/// The inner tree type that does not contain the DML object.
pub struct Inner<R, I, M> {
    root_node: RwLock<R>,
    tree_id: Option<I>,
    msg_action: M,
}

impl<R, I, M> Inner<R, I, M> {
    fn new(tree_id: I, root_node: R, msg_action: M) -> Self {
        Inner {
            tree_id: Some(tree_id),
            root_node: RwLock::new(root_node),
            msg_action,
        }
    }

    /// Returns a new read-only tree.
    pub fn new_ro(root_node: R, msg_action: M) -> Self {
        Inner {
            tree_id: None,
            root_node: RwLock::new(root_node),
            msg_action,
        }
    }

    /// Sets a new root node.
    pub fn update_root_node(&mut self, root_node: R) {
        *self.root_node.get_mut() = root_node;
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>> + From<Inner<X::ObjectRef, X::Info, M>>,
{
    /// Returns a new, empty tree.
    pub fn empty_tree(
        tree_id: X::Info,
        msg_action: M,
        dml: X,
        storage_preference: StoragePreference,
    ) -> Self {
        let root_node = dml.insert(Node::empty_leaf(), tree_id);
        Tree::new(root_node, tree_id, msg_action, dml, storage_preference)
    }

    /// Opens a tree identified by the given root node.
    pub fn open(
        tree_id: X::Info,
        root_node_ptr: X::ObjectPointer,
        msg_action: M,
        dml: X,
        storage_preference: StoragePreference,
    ) -> Self {
        Tree::new(
            X::ref_from_ptr(root_node_ptr),
            tree_id,
            msg_action,
            dml,
            storage_preference,
        )
    }

    fn new(
        root_node: R,
        tree_id: X::Info,
        msg_action: M,
        dml: X,
        storage_preference: StoragePreference,
    ) -> Self {
        Tree {
            inner: I::from(Inner::new(tree_id, root_node, msg_action)),
            dml,
            evict: true,
            marker: PhantomData,
            storage_preference,
        }
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    /// Returns the inner of the tree.
    pub fn inner(&self) -> &I {
        &self.inner
    }

    /// Returns a new tree with the given inner.
    pub fn from_inner(
        inner: I,
        dml: X,
        evict: bool,
        storage_preference: StoragePreference,
    ) -> Self {
        Tree {
            inner,
            dml,
            evict,
            marker: PhantomData,
            storage_preference,
        }
    }

    /// Returns the DML.
    pub fn dmu(&self) -> &X {
        &self.dml
    }

    /// Locks the root node.
    /// Returns `None` if the root node is modified.
    pub fn try_lock_root(&self) -> Option<OwningRef<RwLockWriteGuard<R>, X::ObjectPointer>> {
        let guard = self.inner.borrow().root_node.write();
        OwningRef::new(guard)
            .try_map(|guard| guard.get_unmodified().ok_or(()))
            .ok()
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    fn tree_id(&self) -> X::Info {
        self.inner
            .borrow()
            .tree_id
            .expect("Mutating operations called on read only tree")
    }

    fn msg_action(&self) -> &M {
        &self.inner.borrow().msg_action
    }

    fn get_mut_root_node(&self) -> Result<X::CacheValueRefMut, Error> {
        if let Some(node) = self.dml.try_get_mut(&self.inner.borrow().root_node.read()) {
            return Ok(node);
        }
        Ok(self
            .dml
            .get_mut(&mut self.inner.borrow().root_node.write(), self.tree_id())?)
    }

    fn get_root_node(&self) -> Result<X::CacheValueRef, Error> {
        self.get_node(&self.inner.borrow().root_node)
    }

    fn get_node(&self, np_ref: &RwLock<X::ObjectRef>) -> Result<X::CacheValueRef, Error> {
        if let Some(node) = self.dml.try_get(&np_ref.read()) {
            return Ok(node);
        }
        Ok(self.dml.get(&mut np_ref.write())?)
    }

    fn try_get_mut_node(&self, np_ref: &mut RwLock<X::ObjectRef>) -> Option<X::CacheValueRefMut> {
        self.dml.try_get_mut(np_ref.get_mut())
    }

    fn get_mut_node_mut(&self, np_ref: &mut X::ObjectRef) -> Result<X::CacheValueRefMut, Error> {
        if let Some(node) = self.dml.try_get_mut(np_ref) {
            return Ok(node);
        }
        Ok(self.dml.get_mut(np_ref, self.tree_id())?)
    }

    fn get_mut_node(
        &self,
        np_ref: &mut RwLock<X::ObjectRef>,
    ) -> Result<X::CacheValueRefMut, Error> {
        self.get_mut_node_mut(np_ref.get_mut())
    }

    /*fn walk_tree(
        &self,
        mut node: X::CacheValueRefMut,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<(), Error> {
        // TODO rebalance/merge nodes
        loop {
            let (size_delta, next_node) = {
                let level = node.level();
                let (size_delta, children) = node.range_delete(start, end);
                let (l_np, r_np, dead) = match children {
                    None => return Ok(()),
                    Some((l_np, r_np, dead)) => (l_np, r_np, dead),
                };
                if level == 1 {
                    // is leaf, has no references
                    for np in dead {
                        self.dml.remove(np);
                    }
                } else {
                    // is internal, has children
                    for np in dead {
                        self.remove_subtree(np)?;
                    }
                }
                if let Some(np) = r_np {
                    self.walk_tree(self.get_mut_node_mut(np)?, start, end)?;
                }
                (size_delta, self.get_mut_node_mut(l_np)?)
            };
            node.add_size(size_delta);
            node = next_node;
        }
    }

    fn remove_subtree(&self, np: X::ObjectRef) -> Result<(), Error> {
        let mut nps = vec![np];
        while let Some(np) = nps.pop() {
            {
                let mut node = self.dml.get_and_remove(np)?;
                let level = node.level();
                let result = node.drain_children();
                if let Some(child_nps) = result {
                    if level == 1 {
                        // No need to fetch the leaves
                        for np in child_nps {
                            self.dml.remove(np);
                        }
                    } else {
                        nps.extend(child_nps);
                    }
                }
            }
        }
        Ok(())
    }*/

    #[allow(missing_docs)]
    #[cfg(feature = "internal-api")]
    pub fn tree_dump(&self) -> Result<impl serde::Serialize, Error>
    where
        X::ObjectRef: HasStoragePreference,
    {
        let root = self.get_root_node()?;

        Ok(root.node_info(&self.dml))
    }

    //    pub fn is_modified(&mut self) -> bool {
    //        self.inner.borrow_mut().root_node.is_modified()
    //    }

    pub(crate) fn get_with_info<K: Borrow<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<(KeyInfo, SlicedCowBytes)>, Error> {
        let key = key.borrow();
        let mut msgs = Vec::new();
        let mut node = self.get_root_node()?;
        let data = loop {
            let next_node = match node.get(key, &mut msgs) {
                GetResult::NextNode(np) => self.get_node(np)?,
                GetResult::Data(data) => break data,
            };
            node = next_node;
        };

        match data {
            None => Ok(None),
            Some((info, data)) => {
                let mut tmp = Some(data);
                for (_keyinfo, msg) in msgs.into_iter().rev() {
                    self.msg_action().apply(key, &msg, &mut tmp);
                }

                // This may never be false.
                let data = tmp.unwrap();

                drop(node);
                if self.evict {
                    self.dml.evict()?;
                }
                Ok(Some((info, data)))
            }
        }
    }
}

// impl<X, R, M> Tree<X, M, Arc<Inner<X::ObjectRef, X::Info, M>>>
// where
//     X: HandlerDml<Object = Node<Message<M>, R>, ObjectRef = R>,
//     R: ObjectRef,
//     M: MessageAction,
// {
//     pub fn is_modified(&mut self) -> Option<bool> {
// Arc::get_mut(&mut self.inner).map(|inner|
// inner.root_node.is_modified())
//     }
// }

impl<X, R, M, I> TreeBaseLayer<M> for Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    fn get<K: Borrow<[u8]>>(&self, key: K) -> Result<Option<SlicedCowBytes>, Error> {
        self.get_with_info(key).map(|res| {
            match res {
                None => None,
                // Throw away info..
                Some((_info, data)) => Some(data),
            }
        })
    }

    fn insert<K>(
        &self,
        key: K,
        msg: SlicedCowBytes,
        storage_preference: StoragePreference,
    ) -> Result<(), Error>
    where
        K: Borrow<[u8]> + Into<CowBytes>,
    {
        ensure!(!key.borrow().is_empty(), ErrorKind::EmptyKey);

        let mut parent = None;
        let mut node = {
            let mut node = self.get_mut_root_node()?;
            loop {
                match Ref::try_new(node, |node| node.try_walk(key.borrow())) {
                    Ok(mut child_buffer) => {
                        if let Some(child) = self.try_get_mut_node(child_buffer.node_pointer_mut())
                        {
                            node = child;
                            parent = Some(child_buffer);
                        } else {
                            break child_buffer.into_owner();
                        }
                    }
                    Err(node) => break node,
                };
            }
        };

        let op_preference = storage_preference.or(self.storage_preference);
        let added_size = node.insert(key, msg, self.msg_action(), op_preference);
        node.add_size(added_size);

        if parent.is_none() && node.root_needs_merge() {
            // TODO merge
            unimplemented!();
        }

        self.fixup_foo(node, parent)?;

        // TODO evict?
        // Is this really necessary here? Or is waiting until the next sync fine?
        if self.evict {
            self.dml.evict()?;
        }
        Ok(())
    }

    fn depth(&self) -> Result<u32, Error> {
        Ok(self.get_root_node()?.level() + 1)
    }
}

impl<X, R, M, I> TreeLayer<M> for Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    type Pointer = X::ObjectPointer;

    type Range = RangeIterator<X, M, I>;

    fn range<K, T>(&self, range: T) -> Result<Self::Range, Error>
    where
        T: RangeBounds<K>,
        K: Borrow<[u8]> + Into<CowBytes>,
        Self: Clone,
    {
        ensure!(is_inclusive_non_empty(&range), ErrorKind::InvalidRange);
        Ok(RangeIterator::new(range, self.clone()))
    }

    fn sync(&self) -> Result<Self::Pointer, Error> {
        trace!("sync: Enter");
        let obj_ptr = self
            .dml
            .write_back(|| self.inner.borrow().root_node.write())?;
        trace!("sync: Finished write_back");
        Ok(obj_ptr)
    }
}

impl<X, R, M, I> Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    /// An iterator returning all [ObjectPointer]s of a specified tree.
    /// CAUTION: While build lazily the consumtion of this iterator implies hevay IO work on large trees as nodes up until the leaves (excluded) are read.
    /// This can contain quite a substantial part of data but on the lesser side of the bulk of the data.
    pub fn node_iter<'a>(&'a self) -> TreeIterator<'a, X, R, M, I> {
        TreeIterator {
            tree: self,
            depth: self.depth().unwrap(),
            node: vec![(None, Some(self.get_root_node().unwrap()))].into(),
        }
    }
}

pub struct TreeIterator<'a, X, R, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    tree: &'a Tree<X, M, I>,
    depth: u32,
    node: VecDeque<(
        Option<X::ObjectPointer>,
        Option<<X as HandlerDml>::CacheValueRef>,
    )>,
}

impl<'a, X, R, M, I> Iterator for TreeIterator<'a, X, R, M, I>
where
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    X: Dml<Object = Node<R>, ObjectRef = R>,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    type Item = X::ObjectPointer;

    fn next(&mut self) -> Option<Self::Item> {
        match self.node.pop_front() {
            // valid object ptr & valid node
            Some((objptr, Some(n))) => {
                debug!("Level of node {}", n.level());
                debug!("Total depth {}", self.depth);
                if let Some(iter) = n.child_pointer_iter() {
                    for (node, ptr) in iter.filter_map(|elem| {
                        elem.read()
                            .get_unmodified()
                            .and_then(|np| Some((elem, np.clone())))
                    }) {
                        if n.level() >= self.depth - 1 {
                            self.node.push_back((Some(ptr), None));
                        } else {
                            if let Ok(node) = self.tree.get_node(node) {
                                self.node.push_back((Some(ptr), Some(node)));
                            }
                        }
                    }
                }
                // Prevent early none (for root)
                if let None = &objptr {
                    return self.next();
                }
                objptr
            }
            // valid object ptr & invalid node (leaf)
            Some((objptr, None)) => objptr,
            _ => None,
        }
    }
}

impl<X, R, M, I> ErasedTreeSync for Tree<X, M, I>
where
    X: Dml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<R, X::Info, M>>,
{
    type Pointer = X::ObjectPointer;
    type ObjectRef = R;
    fn erased_sync(&self) -> Result<Self::Pointer, Error> {
        TreeLayer::sync(self)
    }
    fn erased_try_lock_root(
        &self,
    ) -> Option<OwningRef<RwLockWriteGuard<Self::ObjectRef>, Self::Pointer>> {
        self.try_lock_root()
    }
}

mod child_buffer;
mod flush;
mod internal;
mod leaf;
mod node;
mod packed;
mod range;
mod split;

pub use self::{node::Node, range::RangeIterator};
