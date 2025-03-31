use crate::handler::common::{s3_error::S3Error, signature::checksum::ChecksumValue};
use crate::BlobId;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

pub type HeaderList = Vec<(String, String)>;

#[derive(Archive, Deserialize, Serialize, PartialEq, Debug)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub version_id: Uuid, // v4
    pub block_size: u32,
    pub state: ObjectState,
}

pub fn gen_version_id() -> Uuid {
    Uuid::new_v4()
}

impl ObjectLayout {
    pub const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

    #[inline]
    pub fn blob_id(&self) -> Result<BlobId, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.blob_id),
            _ => Err(S3Error::InvalidObjectState),
        }
    }

    #[inline]
    pub fn size(&self) -> Result<u64, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.size),
            ObjectState::Mpu(MpuState::Completed { size, .. }) => Ok(size),
            _ => Err(S3Error::InvalidObjectState),
        }
    }

    #[inline]
    pub fn etag(&self) -> Result<String, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.etag.clone()),
            ObjectState::Mpu(MpuState::Completed { ref etag, .. }) => Ok(etag.clone()),
            _ => Err(S3Error::InvalidObjectState),
        }
    }

    #[inline]
    pub fn num_blocks(&self) -> Result<usize, S3Error> {
        Ok(self.size()?.div_ceil(self.block_size as u64) as usize)
    }

    #[inline]
    pub fn checksum(&self) -> Result<Option<ChecksumValue>, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.checksum),
            ObjectState::Mpu(_) => Ok(None), // TODO
        }
    }

    #[inline]
    pub fn headers(&self) -> Result<&HeaderList, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(&data.headers),
            ObjectState::Mpu(MpuState::Completed { ref headers, .. }) => Ok(headers),
            _ => Err(S3Error::InvalidObjectState),
        }
    }
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
pub enum ObjectState {
    Normal(ObjectData),
    Mpu(MpuState),
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
pub enum MpuState {
    Uploading,
    Aborted,
    Completed {
        size: u64,
        etag: String,
        headers: HeaderList,
    },
}

/// Data stored in normal object or mpu parts
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
pub struct ObjectData {
    pub size: u64,
    pub etag: String,
    pub blob_id: BlobId,
    pub headers: HeaderList,
    pub checksum: Option<ChecksumValue>,
}
