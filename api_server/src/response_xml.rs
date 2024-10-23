use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone, Copy, Default)]
pub struct Xml<T>(pub T);

impl<T> IntoResponse for Xml<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        let mut xml = r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string();
        match quick_xml::se::to_writer(&mut xml, &self.0) {
            Ok(()) => (
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/xml"),
                )],
                xml,
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )],
                err.to_string(),
            )
                .into_response(),
        }
    }
}
