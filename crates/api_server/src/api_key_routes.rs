use axum::extract::Json;
use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

use crate::AppState;
use data_types::{ApiKey, TraceId, Versioned};

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct ApiKeyResponse {
    pub key_id: String,
    pub secret_key: String,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

impl From<ApiKey> for ApiKeyResponse {
    fn from(api_key: ApiKey) -> Self {
        ApiKeyResponse {
            key_id: api_key.key_id,
            secret_key: api_key.secret_key,
            name: api_key.name,
        }
    }
}

pub async fn create_api_key(
    State(app): State<Arc<AppState>>,
    Json(payload): Json<CreateApiKeyRequest>,
) -> Result<Json<ApiKeyResponse>, (StatusCode, Json<ErrorResponse>)> {
    info!("Creating API key with name: {}", payload.name);
    let trace_id = TraceId::new();
    let api_key = Versioned::new(0, ApiKey::new(&payload.name));
    let _key_id = api_key.data.key_id.clone();
    let _serialized_api_key = serde_json::to_string(&api_key.data).map_err(|e| {
        error!("Failed to serialize API key: {:?}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "InternalError".to_string(),
                message: "Failed to serialize API key".to_string(),
            }),
        )
    })?;

    app.put_api_key(&api_key, &trace_id).await.map_err(|e| {
        error!("Failed to put API key to RSS: {:?}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "InternalError".to_string(),
                message: format!("Failed to put API key to RSS: {e:?}"),
            }),
        )
    })?;

    Ok(Json(api_key.data.into()))
}

pub async fn delete_api_key(
    State(app): State<Arc<AppState>>,
    Path(key_id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let key_id = key_id.trim_start_matches("api_key:").to_string();
    info!("Deleting API key with key_id: {}", key_id);
    let trace_id = TraceId::new();
    let api_key = app.get_api_key(key_id, &trace_id).await.map_err(|e| {
        error!("Failed to get API key from RSS: {e:?}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "InternalError".to_string(),
                message: format!("Failed to get API key from RSS: {e:?}"),
            }),
        )
    })?;
    app.delete_api_key(&api_key.data, &trace_id)
        .await
        .map_err(|e| {
            error!("Failed to delete API key from RSS: {e:?}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    code: "InternalError".to_string(),
                    message: format!("Failed to delete API key from RSS: {e:?}"),
                }),
            )
        })?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_api_keys(
    State(app): State<Arc<AppState>>,
) -> Result<Json<Vec<ApiKeyResponse>>, (StatusCode, Json<ErrorResponse>)> {
    info!("Listing API keys");
    let trace_id = TraceId::new();
    let api_keys = app.list_api_keys(&trace_id).await.map_err(|e| {
        error!("Failed to list API keys from RSS: {:?}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                code: "InternalError".to_string(),
                message: format!("Failed to list API keys from RSS: {e:?}"),
            }),
        )
    })?;

    let mut api_key_responses = Vec::new();
    for api_key in api_keys {
        api_key_responses.push(api_key.into());
    }

    Ok(Json(api_key_responses))
}
