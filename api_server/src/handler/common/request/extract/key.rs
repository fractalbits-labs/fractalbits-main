use std::convert::Infallible;

use axum::{extract::FromRequestParts, http::request::Parts};

pub struct KeyFromPath(pub String);

impl<S> FromRequestParts<S> for KeyFromPath
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.uri.path() {
            "/" => Ok(Self("/".into())),
            key => {
                // nss requires '\0' for now
                let mut key = key.to_owned();
                key.push('\0');
                Ok(Self(key))
            }
        }
    }
}
