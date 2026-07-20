use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::http::{HeaderMap, header};
use chrono::{DateTime, Duration, Utc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone)]
pub struct AuthManager {
    admin_hash: Arc<String>,
    sessions: Arc<RwLock<HashMap<String, UiSession>>>,
    secure_cookies: bool,
}

#[derive(Debug, Clone)]
pub struct UiSession {
    pub id: String,
    pub csrf: String,
    pub expires_at: DateTime<Utc>,
}

impl AuthManager {
    pub fn new(admin_token: &str, secure_cookies: bool) -> Result<Self> {
        Ok(Self {
            admin_hash: Arc::new(hash_secret(admin_token)?),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            secure_cookies,
        })
    }

    pub fn verify_secret(&self, candidate: &str) -> bool {
        verify_secret(&self.admin_hash, candidate)
    }

    pub fn verify_bearer(&self, headers: &HeaderMap) -> bool {
        bearer(headers).is_some_and(|token| self.verify_secret(token))
    }

    pub async fn create_session(&self) -> UiSession {
        let session = UiSession {
            id: Uuid::new_v4().to_string(),
            csrf: Uuid::new_v4().to_string(),
            expires_at: Utc::now() + Duration::hours(12),
        };
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());
        session
    }

    pub async fn session(&self, headers: &HeaderMap) -> Option<UiSession> {
        let id = cookie(headers, "scheduler_session")?;
        let mut sessions = self.sessions.write().await;
        sessions.retain(|_, session| session.expires_at > Utc::now());
        sessions.get(id).cloned()
    }

    pub async fn logout(&self, headers: &HeaderMap) {
        if let Some(id) = cookie(headers, "scheduler_session") {
            self.sessions.write().await.remove(id);
        }
    }

    pub fn session_cookie(&self, session: &UiSession) -> String {
        format!(
            "scheduler_session={}; Path=/; HttpOnly; SameSite=Strict; Max-Age=43200{}",
            session.id,
            if self.secure_cookies { "; Secure" } else { "" }
        )
    }

    pub fn expired_cookie(&self) -> String {
        format!(
            "scheduler_session=deleted; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{}",
            if self.secure_cookies { "; Secure" } else { "" }
        )
    }
}

pub fn hash_secret(secret: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|error| anyhow::anyhow!("secret hashing failed: {error}"))?
        .to_string())
}

pub fn verify_secret(hash: &str, candidate: &str) -> bool {
    PasswordHash::new(hash).ok().is_some_and(|hash| {
        Argon2::default()
            .verify_password(candidate.as_bytes(), &hash)
            .is_ok()
    })
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(&format!("{name}=")))
}
