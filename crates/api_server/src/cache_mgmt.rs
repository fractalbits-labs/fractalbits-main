use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{info, warn};

use crate::AppState;

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheInvalidationResponse {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AzStatusUpdateRequest {
    pub status: String,
}

/// Invalidate a specific bucket from the cache
pub async fn invalidate_bucket(
    State(app): State<Arc<AppState>>,
    Path(bucket_name): Path<String>,
) -> impl IntoResponse {
    info!("Invalidating bucket cache for: {}", bucket_name);

    let cache_key = format!("bucket:{}", bucket_name);
    app.cache_coordinator.invalidate_entry(&cache_key).await;

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: format!("Bucket '{}' cache invalidated", bucket_name),
    };

    (StatusCode::OK, Json(response))
}

/// Invalidate a specific API key from the cache
pub async fn invalidate_api_key(
    State(app): State<Arc<AppState>>,
    Path(key_id): Path<String>,
) -> impl IntoResponse {
    info!("Invalidating API key cache for: {}", key_id);

    let cache_key = format!("api_key:{}", key_id);
    app.cache_coordinator.invalidate_entry(&cache_key).await;

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: format!("API key '{}' cache invalidated", key_id),
    };

    (StatusCode::OK, Json(response))
}

/// Update az_status cache for a specific AZ with new status value
pub async fn update_az_status(
    State(app): State<Arc<AppState>>,
    Path(az_id): Path<String>,
    Json(request): Json<AzStatusUpdateRequest>,
) -> impl IntoResponse {
    info!(
        "Updating az_status cache for: {} with status: {}",
        az_id, request.status
    );

    if !app.az_status_enabled.load(Ordering::Acquire) {
        let response = CacheInvalidationResponse {
            status: "error".to_string(),
            message: "AZ status cache not available for this storage backend".to_string(),
        };

        return (StatusCode::NOT_FOUND, Json(response));
    }

    let cache_key = format!("az_status:{}", az_id);
    app.az_status_coordinator
        .insert(&cache_key, request.status.clone())
        .await;

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: format!(
            "AZ status cache for '{}' updated to '{}'",
            az_id, request.status
        ),
    };

    (StatusCode::OK, Json(response))
}

/// Clear the entire cache
pub async fn clear_cache(State(app): State<Arc<AppState>>) -> impl IntoResponse {
    warn!("Clearing entire cache");

    // Invalidate all entries in the cache
    app.cache_coordinator.invalidate_all();
    if app.az_status_enabled.load(Ordering::Acquire) {
        app.az_status_coordinator.invalidate_all();
    }

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: "All cache entries cleared".to_string(),
    };

    (StatusCode::OK, Json(response))
}

/// Health check endpoint for management API
pub async fn mgmt_health() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "healthy",
            "service": "api_server_cache_management"
        })),
    )
}
