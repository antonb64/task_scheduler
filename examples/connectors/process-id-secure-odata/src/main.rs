use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State, rejection::JsonRejection},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, ETAG, WWW_AUTHENTICATE},
    },
    response::{IntoResponse, Response},
    routing::post,
};
use futures::StreamExt;
use reqwest::{Client, Url, redirect::Policy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const CONNECTOR_API_VERSION: &str = "scheduler.connector/v1";
const RESPONSE_VERSION_HEADER: &str = "x-scheduler-connector-api-version";
const DAILY_GROUP: &str = "daily";
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_ODATA_RESPONSE_BYTES: usize = 1024 * 1024;
const ODATA_SELECT: &str = "Id,WorkbookName";
const ODATA_EXPAND: &str = "ParameterSets($filter=Group eq 'daily';$select=Group,Recipients,SelectionVariant,Responsible,Subject,Body,Pdf,Mailfilter,Query1,Query2,Query3,Query4,Query5,Info)";

#[derive(Clone)]
struct AppState {
    connector_authorization: HeaderValue,
    odata_authorization: HeaderValue,
    odata_base_url: Url,
    client: Client,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchRequest {
    api_version: String,
    kind: String,
    resource: String,
}

#[derive(Debug, Deserialize)]
struct ODataWorkbook {
    #[serde(rename = "Id")]
    id: i32,
    #[serde(rename = "WorkbookName")]
    workbook_name: String,
    #[serde(rename = "ParameterSets", default)]
    parameter_sets: Vec<ODataParameterSet>,
}

#[derive(Debug, Deserialize)]
struct ODataParameterSet {
    #[serde(rename = "Group")]
    group: String,
    #[serde(rename = "Recipients")]
    recipients: String,
    #[serde(rename = "SelectionVariant")]
    selection_variant: String,
    #[serde(rename = "Responsible")]
    responsible: String,
    #[serde(rename = "Subject")]
    subject: String,
    #[serde(rename = "Body")]
    body: String,
    #[serde(rename = "Pdf")]
    pdf: bool,
    #[serde(rename = "Mailfilter")]
    mailfilter: bool,
    #[serde(rename = "Query1")]
    query1: String,
    #[serde(rename = "Query2")]
    query2: String,
    #[serde(rename = "Query3")]
    query3: String,
    #[serde(rename = "Query4")]
    query4: String,
    #[serde(rename = "Query5")]
    query5: String,
    #[serde(rename = "Info")]
    info: bool,
}

#[derive(Debug, PartialEq, Serialize)]
struct ProcessParameters {
    id: i32,
    workbook_name: String,
    recipients: String,
    selection_variant: String,
    responsible: String,
    subject: String,
    body: String,
    pdf: bool,
    mailfilter: bool,
    query1: String,
    query2: String,
    query3: String,
    query4: String,
    query5: String,
    info: bool,
}

#[derive(Debug)]
struct ConnectorError {
    status: StatusCode,
    authenticate: bool,
}

impl ConnectorError {
    const fn new(status: StatusCode) -> Self {
        Self {
            status,
            authenticate: false,
        }
    }

    const fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            authenticate: true,
        }
    }
}

impl IntoResponse for ConnectorError {
    fn into_response(self) -> Response {
        let mut response = Response::builder().status(self.status);
        if self.authenticate {
            response = response.header(WWW_AUTHENTICATE, "Bearer");
        }
        response.body(Body::empty()).expect("valid empty response")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listen_addr = env_or("CONNECTOR_LISTEN_ADDR", "127.0.0.1:9010")
        .parse::<SocketAddr>()
        .context("CONNECTOR_LISTEN_ADDR must be a socket address")?;
    let connector_token = required_env("PROCESS_ID_CONNECTOR_TOKEN")?;
    let odata_token = required_env("ODATA_BEARER_TOKEN")?;
    let odata_base_url = validated_odata_base_url(&required_env("ODATA_BASE_URL")?)?;
    let timeout_seconds = env_or("ODATA_TIMEOUT_SECONDS", "10")
        .parse::<u64>()
        .context("ODATA_TIMEOUT_SECONDS must be an integer")?;
    if !(1..=60).contains(&timeout_seconds) {
        bail!("ODATA_TIMEOUT_SECONDS must be between 1 and 60");
    }

    let state = AppState {
        connector_authorization: bearer_header(&connector_token)
            .context("PROCESS_ID_CONNECTOR_TOKEN is not a valid bearer token")?,
        odata_authorization: bearer_header(&odata_token)
            .context("ODATA_BEARER_TOKEN is not a valid bearer token")?,
        odata_base_url,
        client: Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(timeout_seconds))
            .redirect(Policy::none())
            .user_agent("task-scheduler-process-id-example-connector/1")
            .build()
            .context("build OData HTTP client")?,
    };

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .context("bind connector listener")?;
    eprintln!("process-id OData connector listening on {listen_addr}");
    axum::serve(listener, app(state))
        .await
        .context("serve connector")
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/v1/artifacts/fetch", post(fetch_artifact))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state)
}

async fn fetch_artifact(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Result<Json<FetchRequest>, JsonRejection>,
) -> Result<Response, ConnectorError> {
    authorize(&headers, &state.connector_authorization)?;
    let Json(request) = request.map_err(|_| ConnectorError::new(StatusCode::BAD_REQUEST))?;
    if request.api_version != CONNECTOR_API_VERSION {
        return Err(ConnectorError::new(StatusCode::BAD_REQUEST));
    }
    if request.kind != "parameters" {
        return Err(ConnectorError::new(StatusCode::FORBIDDEN));
    }

    let workbook_id = parse_resource(&request.resource)?;
    let parameters = fetch_daily_parameters(&state, workbook_id).await?;
    let body = serde_json::to_vec(&parameters)
        .map_err(|_| ConnectorError::new(StatusCode::INTERNAL_SERVER_ERROR))?;
    let etag = format!("\"{:x}\"", Sha256::digest(&body));

    Response::builder()
        .status(StatusCode::OK)
        .header(RESPONSE_VERSION_HEADER, CONNECTOR_API_VERSION)
        .header(CONTENT_TYPE, "application/json")
        .header(ETAG, etag)
        .body(Body::from(body))
        .map_err(|_| ConnectorError::new(StatusCode::INTERNAL_SERVER_ERROR))
}

fn authorize(headers: &HeaderMap, expected: &HeaderValue) -> Result<(), ConnectorError> {
    match headers.get(AUTHORIZATION) {
        Some(actual) if actual == expected => Ok(()),
        _ => Err(ConnectorError::unauthorized()),
    }
}

fn parse_resource(resource: &str) -> Result<i32, ConnectorError> {
    let id = resource
        .strip_prefix("/workbooks/")
        .and_then(|value| value.strip_suffix("/groups/daily"))
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| ConnectorError::new(StatusCode::NOT_FOUND))?;
    id.parse::<i32>()
        .map_err(|_| ConnectorError::new(StatusCode::NOT_FOUND))
}

async fn fetch_daily_parameters(
    state: &AppState,
    workbook_id: i32,
) -> Result<ProcessParameters, ConnectorError> {
    let url = odata_workbook_url(&state.odata_base_url, workbook_id)?;
    let response = state
        .client
        .get(url)
        .header(AUTHORIZATION, state.odata_authorization.clone())
        .send()
        .await
        .map_err(map_upstream_transport)?;

    if response.status() == StatusCode::NOT_FOUND {
        return Err(ConnectorError::new(StatusCode::NOT_FOUND));
    }
    if !response.status().is_success() {
        return Err(ConnectorError::new(StatusCode::BAD_GATEWAY));
    }

    if !response_has_json_content_type(&response)
        || response
            .content_length()
            .is_some_and(|length| length > MAX_ODATA_RESPONSE_BYTES as u64)
    {
        return Err(ConnectorError::new(StatusCode::BAD_GATEWAY));
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(map_upstream_transport)?;
        if body.len().saturating_add(chunk.len()) > MAX_ODATA_RESPONSE_BYTES {
            return Err(ConnectorError::new(StatusCode::BAD_GATEWAY));
        }
        body.extend_from_slice(&chunk);
    }
    let workbook = serde_json::from_slice::<ODataWorkbook>(&body)
        .map_err(|_| ConnectorError::new(StatusCode::BAD_GATEWAY))?;
    map_workbook(workbook, workbook_id)
}

fn response_has_json_content_type(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|media_type| {
            media_type.eq_ignore_ascii_case("application/json")
                || media_type.to_ascii_lowercase().ends_with("+json")
        })
}

fn odata_workbook_url(base_url: &Url, workbook_id: i32) -> Result<Url, ConnectorError> {
    let mut url = base_url
        .join(&format!("Workbooks({workbook_id})"))
        .map_err(|_| ConnectorError::new(StatusCode::INTERNAL_SERVER_ERROR))?;
    url.query_pairs_mut()
        .append_pair("$select", ODATA_SELECT)
        .append_pair("$expand", ODATA_EXPAND);
    Ok(url)
}

fn map_upstream_transport(error: reqwest::Error) -> ConnectorError {
    if error.is_timeout() {
        ConnectorError::new(StatusCode::GATEWAY_TIMEOUT)
    } else {
        ConnectorError::new(StatusCode::BAD_GATEWAY)
    }
}

fn map_workbook(
    workbook: ODataWorkbook,
    requested_id: i32,
) -> Result<ProcessParameters, ConnectorError> {
    if workbook.id != requested_id {
        return Err(ConnectorError::new(StatusCode::BAD_GATEWAY));
    }
    let mut daily_sets = workbook
        .parameter_sets
        .into_iter()
        .filter(|parameters| parameters.group == DAILY_GROUP);
    let daily = daily_sets
        .next()
        .ok_or_else(|| ConnectorError::new(StatusCode::NOT_FOUND))?;
    if daily_sets.next().is_some() {
        return Err(ConnectorError::new(StatusCode::BAD_GATEWAY));
    }

    Ok(ProcessParameters {
        id: workbook.id,
        workbook_name: workbook.workbook_name,
        recipients: daily.recipients,
        selection_variant: daily.selection_variant,
        responsible: daily.responsible,
        subject: daily.subject,
        body: daily.body,
        pdf: daily.pdf,
        mailfilter: daily.mailfilter,
        query1: daily.query1,
        query2: daily.query2,
        query3: daily.query3,
        query4: daily.query4,
        query5: daily.query5,
        info: daily.info,
    })
}

fn validated_odata_base_url(value: &str) -> Result<Url> {
    let mut url = Url::parse(value).context("ODATA_BASE_URL must be an absolute URL")?;
    if url.cannot_be_a_base()
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("ODATA_BASE_URL must be a base URL without credentials, query, or fragment");
    }
    match url.scheme() {
        "https" => {}
        "http" if url.host().is_some_and(is_loopback_host) => {}
        _ => bail!("ODATA_BASE_URL must use HTTPS unless it targets loopback"),
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url)
}

fn is_loopback_host(host: url::Host<&str>) -> bool {
    match host {
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(address) => address.is_loopback(),
        url::Host::Ipv6(address) => address.is_loopback(),
    }
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("{name} is not set"))?;
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn bearer_header(token: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(&format!("Bearer {token}")).context("invalid bearer token")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::Request};
    use tower::ServiceExt;

    fn parameter_set(group: &str) -> ODataParameterSet {
        ODataParameterSet {
            group: group.to_owned(),
            recipients: "operations@example.com".to_owned(),
            selection_variant: "CURRENT".to_owned(),
            responsible: "Ada Lovelace".to_owned(),
            subject: "Daily processing".to_owned(),
            body: "Ready".to_owned(),
            pdf: true,
            mailfilter: false,
            query1: "SELECT 1".to_owned(),
            query2: String::new(),
            query3: String::new(),
            query4: String::new(),
            query5: String::new(),
            info: false,
        }
    }

    #[test]
    fn resource_accepts_only_a_numeric_id_and_daily_group() {
        assert_eq!(parse_resource("/workbooks/42/groups/daily").unwrap(), 42);
        assert!(parse_resource("/workbooks/42/groups/monthly").is_err());
        assert!(parse_resource("/workbooks/42?group=daily").is_err());
        assert!(parse_resource("/workbooks/1%20or%201/groups/daily").is_err());
    }

    #[test]
    fn map_workbook_selects_daily_and_omits_bound_secrets() {
        let workbook = ODataWorkbook {
            id: 42,
            workbook_name: "Daily Processing.xlsm".to_owned(),
            parameter_sets: vec![parameter_set("monthly"), parameter_set("daily")],
        };

        let parameters = map_workbook(workbook, 42).unwrap();
        let json = serde_json::to_value(parameters).unwrap();
        assert_eq!(json["id"], 42);
        assert_eq!(json["workbook_name"], "Daily Processing.xlsm");
        assert!(json.get("bwp_user").is_none());
        assert!(json.get("bwp_password").is_none());
    }

    #[test]
    fn duplicate_daily_parameter_sets_are_rejected() {
        let workbook = ODataWorkbook {
            id: 42,
            workbook_name: "Daily Processing.xlsm".to_owned(),
            parameter_sets: vec![parameter_set("daily"), parameter_set("daily")],
        };

        let error = map_workbook(workbook, 42).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn odata_query_filters_and_projects_the_daily_parameter_set() {
        let base = validated_odata_base_url("https://odata.example.com/v1").unwrap();
        let url = odata_workbook_url(&base, 42).unwrap();
        assert_eq!(url.path(), "/v1/Workbooks(42)");
        let query = url.query_pairs().collect::<Vec<_>>();
        assert!(
            query
                .iter()
                .any(|pair| pair.0 == "$select" && pair.1 == ODATA_SELECT)
        );
        assert!(
            query
                .iter()
                .any(|pair| { pair.0 == "$expand" && pair.1.contains("$filter=Group eq 'daily'") })
        );
    }

    #[tokio::test]
    async fn unauthorized_takes_precedence_over_a_json_error() {
        let state = AppState {
            connector_authorization: bearer_header("scheduler-secret").unwrap(),
            odata_authorization: bearer_header("odata-secret").unwrap(),
            odata_base_url: validated_odata_base_url("http://127.0.0.1:9/odata").unwrap(),
            client: Client::new(),
        };
        let response = app(state)
            .oneshot(
                Request::post("/v1/artifacts/fetch")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[WWW_AUTHENTICATE], "Bearer");
        assert!(
            to_bytes(response.into_body(), 1024)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
