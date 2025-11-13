use axum::{body::Body, response::Response};
use serde::Serialize;

use crate::handler::common::s3_error::S3Error;

static XML_NS_S3: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct XmlnsS3(&'static str);

impl Default for XmlnsS3 {
    fn default() -> Self {
        Self(XML_NS_S3)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Xml<T>(pub T);

// Note we are not implementing `IntoResponse` trait since we want to attach more contexts with
// error cases, to follow the s3 error responses format:
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
impl<T> TryInto<Response> for Xml<T>
where
    T: Serialize,
{
    type Error = S3Error;

    fn try_into(self) -> Result<Response, Self::Error> {
        let mut xml_body = r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string();
        quick_xml::se::to_writer(&mut xml_body, &self.0)?;
        Ok(Response::builder()
            .header("Content-Type", "application/xml")
            .body(Body::from(xml_body))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header;
    use http_body_util::BodyExt;

    #[derive(Debug, Serialize, PartialEq, Eq, Default)]
    #[serde(rename_all = "PascalCase")]
    struct TestCreateSessionOutput {
        #[serde(rename = "@xmlns")]
        xmlns: XmlnsS3,
        credentials: TestCredentials,
    }

    #[derive(Debug, Serialize, PartialEq, Eq, Default)]
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
            xmlns: Default::default(),
            credentials: TestCredentials {
                access_key_id: "test_key".into(),
                expiration: 100,
                secret_access_key: "test_secret".into(),
                session_token: "test_token".into(),
            },
        };
        let resp: Response = Xml(output).try_into().unwrap();
        assert_eq!(
            "application/xml",
            resp.headers().get(header::CONTENT_TYPE).unwrap()
        );

        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let expected = "\
<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<TestCreateSessionOutput xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
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
