use std::{
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::Bytes,
    extract::{OriginalUri, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header::HeaderName},
    middleware::Next,
    response::{Html, IntoResponse, Response},
    routing::any,
};
use futures::FutureExt;
use scheduler_protocol::control::{
    ManagementRequest, scheduler_control_client::SchedulerControlClient,
};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tower_http::limit::RequestBodyLimitLayer;
use tracing::{error, instrument, warn};
use uuid::Uuid;

const MANAGEMENT_PROXY_TIMEOUT: Duration = Duration::from_secs(15);
const MANAGEMENT_PROXY_BODY_LIMIT: usize = 2 * 1024 * 1024;
const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone)]
pub struct ProxyState {
    pub client: Arc<RwLock<Option<SchedulerControlClient<Channel>>>>,
}

pub fn router(state: ProxyState) -> Router {
    Router::new()
        .route("/", any(proxy))
        .route("/{*path}", any(proxy))
        .layer(RequestBodyLimitLayer::new(MANAGEMENT_PROXY_BODY_LIMIT))
        .layer(axum::middleware::from_fn(proxy_request_context))
        .with_state(state)
}

async fn proxy_request_context(mut request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid_request_id(value))
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    request.headers_mut().insert(
        REQUEST_ID_HEADER.clone(),
        HeaderValue::from_str(&request_id).expect("generated request ID"),
    );
    let response = AssertUnwindSafe(next.run(request)).catch_unwind().await;
    let mut response = match response {
        Ok(response) => response,
        Err(_) => {
            error!(%request_id, "agent management proxy panicked");
            stable_proxy_error(StatusCode::INTERNAL_SERVER_ERROR, &request_id)
        }
    };
    response.headers_mut().insert(
        REQUEST_ID_HEADER.clone(),
        HeaderValue::from_str(&request_id).expect("generated request ID"),
    );
    response
}

#[instrument(
    name = "agent.management.proxy",
    skip_all,
    fields(
        request_id = tracing::field::Empty,
        http.status_code = tracing::field::Empty,
        proxy.result = tracing::field::Empty
    )
)]
async fn proxy(
    State(state): State<ProxyState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let request_id = headers
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unavailable")
        .to_owned();
    tracing::Span::current().record("request_id", request_id.as_str());
    let Some(mut client) = state.client.read().await.clone() else {
        observe_proxy(
            started,
            "coordinator_unavailable",
            StatusCode::SERVICE_UNAVAILABLE,
        );
        return stable_proxy_error(StatusCode::SERVICE_UNAVAILABLE, &request_id);
    };
    let mut forwarded = std::collections::HashMap::new();
    for name in [
        "content-type",
        "cookie",
        "if-match",
        "idempotency-key",
        "x-request-id",
    ] {
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
    let response = match tokio::time::timeout(MANAGEMENT_PROXY_TIMEOUT, client.manage(request))
        .await
    {
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(error)) => {
            warn!(%request_id, grpc_code = ?error.code(), "coordinator management proxy failed");
            observe_proxy(started, "upstream_error", StatusCode::BAD_GATEWAY);
            return stable_proxy_error(StatusCode::BAD_GATEWAY, &request_id);
        }
        Err(_) => {
            warn!(%request_id, "coordinator management proxy timed out");
            observe_proxy(started, "timeout", StatusCode::GATEWAY_TIMEOUT);
            return stable_proxy_error(StatusCode::GATEWAY_TIMEOUT, &request_id);
        }
    };
    let status = StatusCode::from_u16(response.status as u16).unwrap_or(StatusCode::BAD_GATEWAY);
    observe_proxy(started, "forwarded", status);
    let mut output = (status, response.body).into_response();
    for (name, value) in response.headers {
        if let (Ok(name), Ok(value)) = (name.parse::<axum::http::HeaderName>(), value.parse()) {
            output.headers_mut().insert(name, value);
        }
    }
    output
}

fn observe_proxy(started: Instant, result: &'static str, status: StatusCode) {
    use opentelemetry::KeyValue;

    let status_class = match status.as_u16() {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    };
    tracing::Span::current().record("http.status_code", status.as_u16());
    tracing::Span::current().record("proxy.result", result);
    let attributes = [
        KeyValue::new("result", result),
        KeyValue::new("status_class", status_class),
    ];
    let meter = opentelemetry::global::meter("scheduler-agent");
    meter
        .u64_counter("scheduler.management.proxy.requests")
        .build()
        .add(1, &attributes);
    meter
        .f64_histogram("scheduler.management.proxy.duration_ms")
        .build()
        .record(started.elapsed().as_secs_f64() * 1_000.0, &attributes);
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn stable_proxy_error(status: StatusCode, request_id: &str) -> Response {
    (
        status,
        Html(format!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Management unavailable</title></head><body><main><h1>Management unavailable</h1><p>The scheduler agent is still running. Use request ID <code>{request_id}</code> to locate the diagnostic log.</p></main></body></html>"
        )),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::{Router, body::Body, http::Request, routing::get};
    use tower::ServiceExt;

    use super::*;

    async fn panic_handler() -> Response {
        panic!("deliberate proxy fault")
    }

    #[tokio::test]
    async fn proxy_panic_is_contained_and_correlated() {
        let app = Router::new()
            .route("/", get(panic_handler))
            .layer(axum::middleware::from_fn(proxy_request_context));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("x-request-id", "proxy-test-9")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response.headers()["x-request-id"], "proxy-test-9");
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("body");
        let body = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(body.contains("proxy-test-9"));
        assert!(!body.contains("deliberate proxy fault"));
    }

    #[tokio::test]
    async fn proxy_router_rejects_oversized_requests_before_forwarding() {
        let state = ProxyState {
            client: Arc::new(RwLock::new(None)),
        };
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/")
                    .body(Body::from(vec![0_u8; MANAGEMENT_PROXY_BODY_LIMIT + 1]))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
