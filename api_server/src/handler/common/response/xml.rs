use axum::http::{header::CONTENT_TYPE, HeaderValue, StatusCode};
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
                [(CONTENT_TYPE, HeaderValue::from_static("application/xml"))],
                xml,
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                )],
                err.to_string(),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[derive(Debug, Serialize, PartialEq, Eq)]
    #[serde(rename_all = "PascalCase")]
    struct TestCreateSessionOutput {
        credentials: TestCredentials,
    }

    #[derive(Debug, Serialize, PartialEq, Eq)]
    #[serde(rename_all = "PascalCase")]
    struct TestCredentials {
        access_key_id: String,
        expiration: u64,
        secret_access_key: String,
        session_token: String,
    }

    #[tokio::test]
    async fn test_response_xml_encode_ok() {
        let output = TestCreateSessionOutput {
            credentials: TestCredentials {
                access_key_id: "test_key".into(),
                expiration: 100,
                secret_access_key: "test_secret".into(),
                session_token: "test_token".into(),
            },
        };
        let resp = Xml(output).into_response();
        assert_eq!("application/xml", resp.headers().get(CONTENT_TYPE).unwrap());

        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let expected = "\
<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<TestCreateSessionOutput>\
<Credentials>\
<AccessKeyId>test_key</AccessKeyId>\
<Expiration>100</Expiration>\
<SecretAccessKey>test_secret</SecretAccessKey>\
<SessionToken>test_token</SessionToken>\
</Credentials>\
</TestCreateSessionOutput>";
        assert_eq!(expected, String::from_utf8(bytes.to_vec()).unwrap());
    }
}
