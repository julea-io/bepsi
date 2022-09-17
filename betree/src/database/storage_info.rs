use crate::vdev::Block;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Space information representation for a singular storage tier.
pub struct StorageInfo {
    /// Remaining free storage in blocks.
    pub free: Block<u64>,
    /// Total storage in blocks.
    pub total: Block<u64>,
}

impl StorageInfo {
    pub fn percent_free(&self) -> f32 {
        self.free.as_u64() as f32 / self.total.as_u64() as f32
    }

    pub fn percent_full(&self) -> f32 {
        1.0 - self.percent_free()
    }

    pub fn block_overshoot(&self, threshold: f32) -> Block<u64> {
        let threshold = threshold.clamp(0.0, 1.0);
        Block(
            (self.total.0 as f32 * (1.0 - threshold) - self.free.0 as f32)
                .ceil()
                .clamp(0.0, f32::MAX) as u64,
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// Atomic version of [StorageInfo].
pub(crate) struct AtomicStorageInfo {
    pub(crate) free: AtomicU64,
    pub(crate) total: AtomicU64,
}

impl From<&AtomicStorageInfo> for StorageInfo {
    fn from(info: &AtomicStorageInfo) -> Self {
        Self {
            free: Block(info.free.load(Ordering::Relaxed)),
            total: Block(info.total.load(Ordering::Relaxed)),
        }
    }
}

impl From<&StorageInfo> for AtomicStorageInfo {
    fn from(info: &StorageInfo) -> Self {
        Self {
            free: AtomicU64::new(info.free.as_u64()),
            total: AtomicU64::new(info.total.as_u64()),
        }
    }
}
