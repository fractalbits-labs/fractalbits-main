use uuid::Uuid;

/// BlobGuid combines blob_id (UUID) with volume_id for multi-BSS support
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize,
)]
pub struct DataBlobGuid {
    pub blob_id: Uuid,
    pub volume_id: u16,
}

impl DataBlobGuid {
    pub const S3_VOLUME: u16 = u16::MAX;
}

impl std::fmt::Display for DataBlobGuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:d{}", self.blob_id, self.volume_id)
    }
}

/// MetadataBlobGuid combines blob_id (UUID) with volume_id for multi-BSS metadata support
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize,
)]
pub struct MetadataBlobGuid {
    pub blob_id: Uuid,
    pub volume_id: u16,
}

impl std::fmt::Display for MetadataBlobGuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:m{}", self.blob_id, self.volume_id)
    }
}
