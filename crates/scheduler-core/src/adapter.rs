use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{CONTENT_TYPE, ETAG, LAST_MODIFIED};
use url::Url;

#[derive(Debug, Clone, Copy)]
pub enum ArtifactKind {
    Blueprint,
    Parameters,
}

#[derive(Debug, Clone)]
pub struct Artifact {
    pub bytes: Vec<u8>,
    pub media_type: Option<String>,
    pub source_version: Option<String>,
}

#[async_trait]
pub trait ArtifactAdapter: Send + Sync {
    async fn fetch(&self, uri: &Url, kind: ArtifactKind) -> Result<Artifact>;
}

#[derive(Clone, Default)]
pub struct AdapterRegistry {
    adapters: HashMap<String, Arc<dyn ArtifactAdapter>>,
}

impl AdapterRegistry {
    pub fn with_defaults(
        allowed_file_roots: Vec<PathBuf>,
        http_headers: HashMap<String, String>,
    ) -> Result<Self> {
        let mut registry = Self::default();
        registry.register("file", Arc::new(FileAdapter::new(allowed_file_roots)?));
        let http = Arc::new(HttpAdapter::new(http_headers)?);
        registry.register("http", http.clone());
        registry.register("https", http);
        Ok(registry)
    }

    pub fn register(&mut self, scheme: impl Into<String>, adapter: Arc<dyn ArtifactAdapter>) {
        self.adapters.insert(scheme.into(), adapter);
    }

    pub async fn fetch(&self, reference: &str, kind: ArtifactKind) -> Result<Artifact> {
        let uri = Url::parse(reference).context("invalid artifact URI")?;
        let adapter = self
            .adapters
            .get(uri.scheme())
            .with_context(|| format!("no artifact adapter registered for {}", uri.scheme()))?;
        adapter.fetch(&uri, kind).await
    }
}

struct FileAdapter {
    roots: Vec<PathBuf>,
}

impl FileAdapter {
    fn new(roots: Vec<PathBuf>) -> Result<Self> {
        let roots = roots
            .into_iter()
            .map(|root| {
                root.canonicalize()
                    .with_context(|| format!("invalid artifact root {}", root.display()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { roots })
    }
}

#[async_trait]
impl ArtifactAdapter for FileAdapter {
    async fn fetch(&self, uri: &Url, kind: ArtifactKind) -> Result<Artifact> {
        let path = uri
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("file artifact URI is not a local path"))?;
        let canonical = tokio::fs::canonicalize(&path)
            .await
            .with_context(|| format!("cannot resolve artifact {}", path.display()))?;
        if !self.roots.iter().any(|root| canonical.starts_with(root)) {
            bail!("artifact path is outside configured file roots");
        }
        let metadata = tokio::fs::metadata(&canonical).await?;
        if metadata.len() > max_size(kind) {
            bail!("artifact exceeds size limit");
        }
        let bytes = tokio::fs::read(&canonical).await?;
        let modified = metadata.modified().ok().and_then(|time| {
            time.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|duration| format!("{}:{}", duration.as_nanos(), metadata.len()))
        });
        Ok(Artifact {
            bytes,
            media_type: canonical
                .extension()
                .and_then(|value| value.to_str())
                .map(extension_media_type)
                .map(str::to_owned),
            source_version: modified,
        })
    }
}

struct HttpAdapter {
    client: reqwest::Client,
}

impl HttpAdapter {
    fn new(headers: HashMap<String, String>) -> Result<Self> {
        let mut default_headers = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            default_headers.insert(
                reqwest::header::HeaderName::try_from(name)?,
                reqwest::header::HeaderValue::try_from(value)?,
            );
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .default_headers(default_headers)
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::limited(3))
                .build()?,
        })
    }
}

#[async_trait]
impl ArtifactAdapter for HttpAdapter {
    async fn fetch(&self, uri: &Url, kind: ArtifactKind) -> Result<Artifact> {
        let response = self
            .client
            .get(uri.clone())
            .send()
            .await
            .context("artifact HTTP transport failed")?;
        let status = response.status();
        if !status.is_success() {
            bail!(
                "artifact HTTP request failed with status {} ({})",
                status.as_u16(),
                status.canonical_reason().unwrap_or("unknown status")
            );
        }
        let length = response.content_length().unwrap_or(0);
        if length > max_size(kind) {
            bail!("artifact exceeds size limit");
        }
        let media_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let source_version = response
            .headers()
            .get(ETAG)
            .or_else(|| response.headers().get(LAST_MODIFIED))
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = response.bytes().await?;
        if bytes.len() as u64 > max_size(kind) {
            bail!("artifact exceeds size limit");
        }
        Ok(Artifact {
            bytes: bytes.to_vec(),
            media_type,
            source_version,
        })
    }
}

fn max_size(kind: ArtifactKind) -> u64 {
    match kind {
        ArtifactKind::Blueprint => 1_048_576,
        ArtifactKind::Parameters => 4_194_304,
    }
}

fn extension_media_type(extension: &str) -> &'static str {
    match extension.to_ascii_lowercase().as_str() {
        "yaml" | "yml" => "application/yaml",
        _ => "application/json",
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[tokio::test]
    async fn file_adapter_allows_files_under_an_explicit_root() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("parameters.json");
        std::fs::write(&path, b"{}").expect("fixture");
        let registry = AdapterRegistry::with_defaults(vec![root.path().into()], HashMap::new())
            .expect("registry");

        let artifact = registry
            .fetch(
                url::Url::from_file_path(&path).expect("URL").as_str(),
                ArtifactKind::Parameters,
            )
            .await
            .expect("artifact");
        assert_eq!(artifact.bytes, b"{}");
        assert_eq!(artifact.media_type.as_deref(), Some("application/json"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_adapter_rejects_a_symlink_that_escapes_the_root() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::NamedTempFile::new().expect("outside");
        let link = root.path().join("escape.json");
        std::os::unix::fs::symlink(outside.path(), &link).expect("symlink");
        let registry = AdapterRegistry::with_defaults(vec![root.path().into()], HashMap::new())
            .expect("registry");

        let error = registry
            .fetch(
                url::Url::from_file_path(&link).expect("URL").as_str(),
                ArtifactKind::Parameters,
            )
            .await
            .expect_err("escape must fail");
        assert!(error.to_string().contains("outside configured file roots"));
    }

    #[tokio::test]
    async fn http_adapter_sends_configured_authentication_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let mut request = vec![0; 4096];
            let read = stream.read(&mut request).await.expect("request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.contains("authorization: Bearer artifact-secret\r\n"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: fixture-v1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .await
                .expect("response");
        });
        let mut headers = HashMap::new();
        headers.insert("authorization".into(), "Bearer artifact-secret".into());
        let registry = AdapterRegistry::with_defaults(Vec::new(), headers).expect("registry");

        let artifact = registry
            .fetch(
                &format!("http://{address}/parameters.json"),
                ArtifactKind::Parameters,
            )
            .await
            .expect("artifact");
        assert_eq!(artifact.bytes, b"{}");
        assert_eq!(artifact.source_version.as_deref(), Some("fixture-v1"));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn http_adapter_reports_upstream_status_code() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let mut request = vec![0; 4096];
            let _read = stream.read(&mut request).await.expect("request");
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("response");
        });
        let registry =
            AdapterRegistry::with_defaults(Vec::new(), HashMap::new()).expect("registry");

        let error = registry
            .fetch(
                &format!("http://{address}/blueprint.yaml"),
                ArtifactKind::Blueprint,
            )
            .await
            .expect_err("upstream failure");
        assert!(error.to_string().contains("status 503"));
        server.await.expect("server");
    }
}
