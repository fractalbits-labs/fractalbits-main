mod get_object;
mod get_object_attributes;
mod list_multipart_uploads;
mod list_objects;
mod list_objects_v2;
mod list_parts;

pub use get_object::{
    get_object_content, get_object_handler, override_headers, HeaderOpts as GetObjectHeaderOpts,
    QueryOpts as GetObjectQueryOpts,
};
pub use get_object_attributes::get_object_attributes_handler;
pub use list_multipart_uploads::list_multipart_uploads_handler;
pub use list_objects::list_objects_handler;
pub use list_objects_v2::{list_objects_v2_handler, Object};
pub use list_parts::list_parts_handler;

use super::common::authorization::Authorization;

pub enum GetEndpoint {
    GetObject,
    GetObjectAttributes,
    ListMultipartUploads,
    ListObjects,
    ListObjectsV2,
    ListParts,
}

impl GetEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            GetEndpoint::GetObject => Authorization::Read,
            GetEndpoint::GetObjectAttributes => Authorization::Read,
            GetEndpoint::ListMultipartUploads => Authorization::Read,
            GetEndpoint::ListObjects => Authorization::Read,
            GetEndpoint::ListObjectsV2 => Authorization::Read,
            GetEndpoint::ListParts => Authorization::Read,
        }
    }
}
