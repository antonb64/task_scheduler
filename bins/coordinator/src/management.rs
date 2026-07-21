use std::{
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

use axum::{
    extract::Request,
    http::{HeaderValue, StatusCode, header::HeaderName},
    middleware::Next,
    response::{Html, IntoResponse, Response},
};
use futures::FutureExt;
use serde_json::json;
use tracing::{error, info};
use uuid::Uuid;

pub const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

tokio::task_local! {
    static CURRENT_REQUEST_ID: String;
}

pub async fn request_context(mut request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| valid_request_id(value))
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let is_api = path.starts_with("/api/") || path.starts_with("/hooks/");
    request.headers_mut().insert(
        REQUEST_ID_HEADER.clone(),
        HeaderValue::from_str(&request_id).expect("generated request IDs are valid header values"),
    );

    let started = Instant::now();
    let response = CURRENT_REQUEST_ID
        .scope(request_id.clone(), async move {
            AssertUnwindSafe(next.run(request)).catch_unwind().await
        })
        .await;

    let mut response = match response {
        Ok(response) => response,
        Err(_) => {
            error!(%request_id, %method, %path, "management request panicked");
            panic_response(&request_id, is_api)
        }
    };
    response.headers_mut().insert(
        REQUEST_ID_HEADER.clone(),
        HeaderValue::from_str(&request_id).expect("generated request IDs are valid header values"),
    );
    info!(
        %request_id,
        %method,
        %path,
        status = response.status().as_u16(),
        duration_ms = duration_millis(started.elapsed()),
        "management request completed"
    );
    observe_request(started, response.status());
    response
}

fn observe_request(started: Instant, status: StatusCode) {
    use opentelemetry::KeyValue;

    let status_class = match status.as_u16() {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    };
    let attributes = [KeyValue::new("status_class", status_class)];
    let meter = opentelemetry::global::meter("scheduler-coordinator");
    meter
        .u64_counter("scheduler.management.requests")
        .build()
        .add(1, &attributes);
    meter
        .f64_histogram("scheduler.management.request.duration_ms")
        .build()
        .record(started.elapsed().as_secs_f64() * 1_000.0, &attributes);
}

pub fn current_request_id() -> String {
    CURRENT_REQUEST_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| Uuid::new_v4().to_string())
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn duration_millis(duration: Duration) -> u128 {
    duration.as_millis()
}

fn panic_response(request_id: &str, is_api: bool) -> Response {
    if is_api {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({
                "error": "internal_server_error",
                "request_id": request_id,
            })),
        )
            .into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Html(format!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Request failed</title></head><body><main><h1>Request failed</h1><p>The scheduler remained available. Use request ID <code>{request_id}</code> to find the diagnostic log.</p></main></body></html>"
        )),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::{
        Router, body::Body, http::Request, middleware, response::IntoResponse, routing::get,
    };
    use tower::ServiceExt;

    use super::*;

    async fn panic_handler() -> Response {
        panic!("deliberate UI fault")
    }

    #[tokio::test]
    async fn catches_ui_panics_and_returns_a_correlated_stable_error() {
        let app = Router::new()
            .route("/panic", get(panic_handler))
            .layer(middleware::from_fn(request_context));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/panic")
                    .header("x-request-id", "test-request-42")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response.headers()["x-request-id"], "test-request-42");
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("body");
        let body = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(body.contains("test-request-42"));
        assert!(!body.contains("deliberate UI fault"));
    }

    #[tokio::test]
    async fn replaces_untrusted_request_ids() {
        let app = Router::new()
            .route(
                "/",
                get(|| async { StatusCode::NO_CONTENT.into_response() }),
            )
            .layer(middleware::from_fn(request_context));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("x-request-id", "bad request id with spaces")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let request_id = response.headers()["x-request-id"]
            .to_str()
            .expect("request ID");
        assert_ne!(request_id, "bad request id with spaces");
        assert!(Uuid::parse_str(request_id).is_ok());
    }
}
