use std::sync::Arc;

use axum::{
    Router,
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use scheduler_protocol::control::{
    ManagementRequest, scheduler_control_client::SchedulerControlClient,
};
use tokio::sync::RwLock;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct ProxyState {
    pub client: Arc<RwLock<Option<SchedulerControlClient<Channel>>>>,
}

pub fn router(state: ProxyState) -> Router {
    Router::new()
        .route("/", any(proxy))
        .route("/{*path}", any(proxy))
        .with_state(state)
}

async fn proxy(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(mut client) = state.client.read().await.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Coordinator is unavailable",
        )
            .into_response();
    };
    let mut forwarded = std::collections::HashMap::new();
    for name in ["content-type", "cookie", "if-match", "idempotency-key"] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            forwarded.insert(name.to_owned(), value.to_owned());
        }
    }
    let request = ManagementRequest {
        method: method.as_str().into(),
        path: uri
            .path_and_query()
            .map(|value| value.as_str())
            .unwrap_or("/")
            .into(),
        body: String::from_utf8_lossy(&body).into_owned(),
        headers: forwarded,
    };
    let response = match client.manage(request).await {
        Ok(response) => response.into_inner(),
        Err(error) => return (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
    };
    let status = StatusCode::from_u16(response.status as u16).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut output = (status, response.body).into_response();
    for (name, value) in response.headers {
        if let (Ok(name), Ok(value)) = (name.parse::<axum::http::HeaderName>(), value.parse()) {
            output.headers_mut().insert(name, value);
        }
    }
    output
}
