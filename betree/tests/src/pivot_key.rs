use super::util;
use betree_storage_stack::{
    data_management::HasStoragePreference,
    tree::{NodeInfo, PivotKey},
    Database, DatabaseConfiguration, Dataset,
};
use rand::{seq::IteratorRandom, RngCore};

#[test]
fn structure_is_good() {
    let (_db, ds) = util::random_db(2, 128);
    let dmp = ds.tree_dump().unwrap();
    internal_node_check(&dmp)
}

#[test]
fn get() {
    let (_db, ds) = util::random_db(2, 128);
    let dmp = ds.tree_dump().unwrap();
    let pk = random_pivot_key(&dmp).clone().unwrap();
    let _node = ds.test_get_node_pivot(&pk).unwrap().unwrap();
}

fn random_pivot_key(ni: &NodeInfo) -> Option<&PivotKey> {
    match ni {
        NodeInfo::Internal { children, .. } => {
            let mut rng = rand::thread_rng();
            Some(
                children
                    .iter()
                    .flat_map(|c_buf| [Some(&c_buf.pivot_key), random_pivot_key(&c_buf.child)])
                    .filter_map(|e| e)
                    .choose(&mut rng)
                    .unwrap(),
            )
        }
        // Only inspect Internal nodes as they hold child buffers
        _ => None,
    }
}

fn internal_node_check(ni: &NodeInfo) {
    match ni {
        NodeInfo::Internal { children, .. } => {
            for (idx, c_buf) in children.iter().enumerate() {
                assert!(!c_buf.pivot_key.is_root());
                if idx == 0 {
                    assert!(c_buf.pivot_key.is_left());
                } else {
                    assert!(c_buf.pivot_key.is_right());
                }
                internal_node_check(&c_buf.child)
            }
        }
        // Only inspect Internal nodes as they hold child buffers
        _ => {}
    }
}
