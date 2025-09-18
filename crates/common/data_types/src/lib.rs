mod api_key;
mod blob_guid;
mod bucket;
pub mod hash;
mod permission;
mod volume;

// Re-export the main types for convenience
pub use api_key::ApiKey;
pub use blob_guid::{DataBlobGuid, MetadataBlobGuid};
pub use bucket::Bucket;
pub use permission::BucketKeyPerm;
pub use volume::{BssNode, DataVgInfo, DataVolume, QuorumConfig};

#[derive(Clone)]
pub struct Versioned<T: Sized> {
    pub version: i64,
    pub data: T,
}

impl<T: Sized> Versioned<T> {
    pub fn new(version: i64, data: T) -> Self {
        Self { version, data }
    }
}

impl<T: Sized> From<(i64, T)> for Versioned<T> {
    fn from(value: (i64, T)) -> Self {
        Self {
            version: value.0,
            data: value.1,
        }
    }
}
