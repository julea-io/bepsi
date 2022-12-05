use super::{
    child_buffer::ChildBuffer, internal::TakeChildBuffer, FillUpResult, Inner, Node, Tree,
};
use crate::{
    cache::AddSize,
    data_management::{HandlerDml, HasStoragePreference, ObjectRef},
    size::Size,
    tree::{errors::*, MessageAction},
};
use stable_deref_trait::StableDeref;
use std::{
    borrow::Borrow,
    mem::transmute,
    ops::{Deref, DerefMut},
};

impl<X, R, M, I> Tree<X, M, I>
where
    X: HandlerDml<Object = Node<R>, ObjectRef = R>,
    R: ObjectRef<ObjectPointer = X::ObjectPointer> + HasStoragePreference,
    M: MessageAction,
    I: Borrow<Inner<X::ObjectRef, X::Info, M>>,
{
    /// This method performs necessary flushing and rebalancing operations if
    /// too many entries are stored at a node. We use this method immediately
    /// after a new value is inserted in a the specified node to assure that we
    /// will not end up in a state where an overfull node is serialized onto
    /// disk.
    ///
    /// Brief Summary
    /// -------------
    /// This method performs flushes on a path started by the given `node`.  And
    /// continues down until no more nodes can be found which are larger than
    /// they are allowed to. The basic approach is structured like this:
    ///
    /// ```pseudo
    /// Identifiers: node, child
    ///
    /// 1: Check if we have to split the current node. On success, return if new nodes are okay.
    /// 2: Select child with largest messages.
    /// 3: If the child is an internal node and too large, set child as node, goto 1.
    /// 4: If the child is an internal node and has not enough children, merge child with siblings.
    /// 5: Flush down to child.
    /// 6: If child is leaf and too small, merge with siblings.
    /// 7: If child is leaf and too large, split.
    /// 8: If node is still too large, goto 1.
    /// 9: Set child as node, goto 1.
    /// ```
    pub(super) fn rebalance_tree(
        &self,
        mut node: X::CacheValueRefMut,
        mut parent: Option<Ref<X::CacheValueRefMut, TakeChildBuffer<'static, ChildBuffer<R>>>>,
    ) -> Result<(), Error> {
        loop {
            if !node.is_too_large() {
                return Ok(());
            }
            debug!(
                "{}, {:?}, lvl: {}, size: {}, actual: {:?}",
                node.kind(),
                node.fanout(),
                node.level(),
                node.size(),
                node.actual_size()
            );
            // 1. Select the largest child buffer which can be flushed.
            let mut child_buffer = match Ref::try_new(node, |node| node.try_find_flush_candidate()) {
                // 1.1. If there is none we have to split the node.
                Err(_node) => match parent {
                    None => {
                        self.split_root_node(_node);
                        return Ok(());
                    }
                    Some(ref mut parent) => {
                        let (next_node, size_delta) = self.split_node(_node, parent)?;
                        parent.add_size(size_delta);
                        node = next_node;
                        continue;
                    }
                },
                // 1.2. If successful we flush in the following steps to this node.
                Ok(selected_child_buffer) => selected_child_buffer,
            };
            let mut child = self.get_mut_node(child_buffer.node_pointer_mut())?;
            // 2. Iterate down to child if too large
            if !child.is_leaf() && child.is_too_large() {
                warn!("Aborting flush, child is too large already");
                parent = Some(child_buffer);
                node = child;
                continue;
            }
            // 3. If child is internal, small and has not many children -> merge the children of node.
            if child.has_too_low_fanout() {
                let size_delta = {
                    let mut m = child_buffer.prepare_merge();
                    let mut sibling = self.get_mut_node(m.sibling_node_pointer())?;
                    let is_right_sibling = m.is_right_sibling();
                    let (pivot, old_np, size_delta) = m.merge_children();
                    if is_right_sibling {
                        let size_delta = child.merge(&mut sibling, pivot);
                        child.add_size(size_delta);
                    } else {
                        let size_delta = sibling.merge(&mut child, pivot);
                        child.add_size(size_delta);
                    }
                    self.dml.remove(old_np);
                    size_delta
                };
                child_buffer.add_size(size_delta);
                node = child_buffer.into_owner();
                continue;
            }
            // 4. Remove messages from the child buffer.
            let (buffer, size_delta) = child_buffer.take_buffer();
            child_buffer.add_size(size_delta);
            self.dml.verify_cache();
            // 5. Insert messages from the child buffer into the child.
            let size_delta_child = child.insert_msg_buffer(buffer, self.msg_action());
            child.add_size(size_delta_child);

            // 6. Check if minimal leaf size is fulfilled, otherwise merge again.
            if child.is_too_small_leaf() {
                let size_delta = {
                    let mut m = child_buffer.prepare_merge();
                    let mut sibling = self.get_mut_node(m.sibling_node_pointer())?;
                    // TODO size delta for child/sibling
                    // TODO deallocation
                    let result = if m.is_right_sibling() {
                        child.leaf_rebalance(&mut sibling)
                    } else {
                        sibling.leaf_rebalance(&mut child)
                    };
                    match result {
                        FillUpResult::Merged => m.merge_children().2,
                        FillUpResult::Rebalanced(pivot) => m.rebalanced(pivot),
                    }
                };
                child_buffer.add_size(size_delta);
            }
            // 7. If the child is too large, split until it is not.
            while child.is_too_large_leaf() {
                let (next_node, size_delta) = self.split_node(child, &mut child_buffer)?;
                child_buffer.add_size(size_delta);
                child = next_node;
            }

            // 8. After finishing all operations once, see if they have to be repeated.
            if child_buffer.size() > super::MAX_INTERNAL_NODE_SIZE {
                warn!("Node is still too large");
                if child.is_too_large() {
                    warn!("... but child, too");
                }
                node = child_buffer.into_owner();
                continue;
            }
            // 9. Traverse down to child.
            // Drop old parent here.
            parent = Some(child_buffer);
            node = child;
        }
    }
}

pub struct Ref<T, U> {
    inner: U,
    owner: T,
}

impl<T: StableDeref + DerefMut, U> Ref<T, TakeChildBuffer<'static, U>> {
    pub fn try_new<F>(mut owner: T, f: F) -> Result<Self, T>
    where
        F: for<'a> FnOnce(&'a mut T::Target) -> Option<TakeChildBuffer<'a, U>>,
    {
        match unsafe { transmute(f(&mut owner)) } {
            None => Err(owner),
            Some(inner) => Ok(Ref { owner, inner }),
        }
    }

    pub fn into_owner(self) -> T {
        self.owner
    }
}

impl<T: AddSize, U> AddSize for Ref<T, U> {
    fn add_size(&self, size_delta: isize) {
        self.owner.add_size(size_delta);
    }
}

impl<T, U> Deref for Ref<T, U> {
    type Target = U;
    fn deref(&self) -> &U {
        &self.inner
    }
}

impl<T, U> DerefMut for Ref<T, U> {
    fn deref_mut(&mut self) -> &mut U {
        &mut self.inner
    }
}
