mod head_object;

pub use head_object::head_object_handler;

use super::common::authorization::Authorization;

pub enum HeadEndpoint {
    HeadObject,
}

impl HeadEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            HeadEndpoint::HeadObject => Authorization::Read,
        }
    }
}
