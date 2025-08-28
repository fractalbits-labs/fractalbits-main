pub mod api_key_table;
pub mod bucket_table;
pub mod permission;
pub mod table;

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
