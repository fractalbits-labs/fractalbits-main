use crate::BlobId;
use rand::RngCore;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub version_id: [u8; 16],
    pub state: ObjectState,
}

pub fn gen_version_id() -> [u8; 16] {
    let mut bytes = [0; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

impl ObjectLayout {
    pub fn blob_id(&self) -> BlobId {
        match self.state {
            ObjectState::Normal(ref data) => data.blob_id,
            ObjectState::Mpu(_) => todo!(),
        }
    }

    pub fn size(&self) -> u64 {
        match self.state {
            ObjectState::Normal(ref data) => data.size,
            ObjectState::Mpu(_) => todo!(),
        }
    }
}

#[derive(Archive, Deserialize, Serialize)]
pub enum ObjectState {
    Normal(ObjectData),
    Mpu(MpuState),
}

#[derive(Archive, Deserialize, Serialize)]
pub enum MpuState {
    Uploading,
    Aborted,
    Completed {
        size: u64,
        etag: String,
        part_number: u16,
    },
}

/// Data stored in normal object or mpu parts
#[derive(Archive, Deserialize, Serialize)]
pub struct ObjectData {
    pub size: u64,
    pub etag: String,
    pub blob_id: BlobId,
}
