// Replicated from api_server::object_layout - must match rkyv layout exactly
// for deserialization of NSS-stored objects.

use data_types::DataBlobGuid;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::error::FuseError;

pub type HeaderList = Vec<(String, String)>;

#[derive(Archive, Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub version_id: Uuid,
    pub block_size: u32,
    pub state: ObjectState,
}

impl ObjectLayout {
    pub const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

    pub fn is_listable(&self) -> bool {
        matches!(
            &self.state,
            ObjectState::Normal(_) | ObjectState::Mpu(MpuState::Completed(_))
        )
    }

    pub fn blob_guid(&self) -> Result<DataBlobGuid, FuseError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.blob_guid),
            _ => Err(FuseError::InvalidState),
        }
    }

    pub fn size(&self) -> Result<u64, FuseError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.size),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => Ok(core_meta_data.size),
            _ => Err(FuseError::InvalidState),
        }
    }

    pub fn num_blocks(&self) -> Result<usize, FuseError> {
        Ok(self.size()?.div_ceil(self.block_size as u64) as usize)
    }
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub enum ObjectState {
    Normal(ObjectMetaData),
    Mpu(MpuState),
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub enum MpuState {
    Uploading,
    Aborted,
    Completed(ObjectCoreMetaData),
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct ObjectMetaData {
    pub blob_guid: DataBlobGuid,
    pub core_meta_data: ObjectCoreMetaData,
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct ObjectCoreMetaData {
    pub size: u64,
    pub etag: String,
    pub headers: HeaderList,
    pub checksum: Option<ChecksumValue>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Copy,
    Debug,
)]
pub enum ChecksumValue {
    Crc32([u8; 4]),
    Crc32c([u8; 4]),
    Crc64Nvme([u8; 8]),
    Sha1([u8; 20]),
    Sha256([u8; 32]),
}
