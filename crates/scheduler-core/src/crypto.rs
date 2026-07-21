use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use zeroize::Zeroizing;

#[derive(Clone)]
pub struct SnapshotCipher {
    key: Zeroizing<[u8; 32]>,
    key_id: String,
}

impl SnapshotCipher {
    pub fn from_base64(key_id: impl Into<String>, encoded: &str) -> Result<Self> {
        let decoded = STANDARD
            .decode(encoded)
            .context("master key is not valid base64")?;
        if decoded.len() != 32 {
            bail!("master key must decode to exactly 32 bytes");
        }
        let mut key = Zeroizing::new([0_u8; 32]);
        key.copy_from_slice(&decoded);
        Ok(Self {
            key,
            key_id: key_id.into(),
        })
    }

    pub fn generate_base64() -> String {
        let mut key = [0_u8; 32];
        OsRng.fill_bytes(&mut key);
        STANDARD.encode(key)
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = XChaCha20Poly1305::new((&*self.key).into());
        let mut nonce_bytes = [0_u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let mut output = nonce_bytes.to_vec();
        output.extend(
            cipher
                .encrypt(XNonce::from_slice(&nonce_bytes), plaintext)
                .map_err(|_| anyhow::anyhow!("snapshot encryption failed"))?,
        );
        Ok(output)
    }

    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < 24 {
            bail!("encrypted snapshot is truncated");
        }
        let (nonce, payload) = ciphertext.split_at(24);
        XChaCha20Poly1305::new((&*self.key).into())
            .decrypt(XNonce::from_slice(nonce), payload)
            .map_err(|_| anyhow::anyhow!("snapshot decryption failed"))
    }

    /// Produces a stable, keyed identifier for correlating the same non-secret
    /// input without making low-entropy parameter values enumerable from the
    /// database. Callers must pass only the non-secret parameter projection.
    pub fn input_fingerprint(
        &self,
        blueprint_digest: &str,
        nonsecret_parameters: &Value,
    ) -> Result<String> {
        let canonical = canonical_json(nonsecret_parameters)?;
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&*self.key)
            .map_err(|_| anyhow::anyhow!("cannot initialize input fingerprint"))?;
        mac.update(b"task-scheduler-input-fingerprint-v1\0");
        mac.update(blueprint_digest.as_bytes());
        mac.update(b"\0");
        mac.update(&canonical);
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

fn canonical_json(value: &Value) -> Result<Vec<u8>> {
    fn sort(value: &Value) -> Value {
        match value {
            Value::Object(values) => Value::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), sort(value)))
                    .collect(),
            ),
            Value::Array(values) => Value::Array(values.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    Ok(serde_json::to_vec(&sort(value))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let cipher = SnapshotCipher::from_base64("test", &SnapshotCipher::generate_base64())
            .expect("cipher");
        let encrypted = cipher.encrypt(b"secret").expect("encrypt");
        assert_ne!(encrypted, b"secret");
        assert_eq!(cipher.decrypt(&encrypted).expect("decrypt"), b"secret");
    }

    #[test]
    fn input_fingerprint_is_keyed_and_object_order_independent() {
        let first = SnapshotCipher::from_base64("first", &SnapshotCipher::generate_base64())
            .expect("cipher");
        let second = SnapshotCipher::from_base64("second", &SnapshotCipher::generate_base64())
            .expect("cipher");
        let left = serde_json::json!({"month": "2026-07", "customer": 42});
        let right: Value =
            serde_json::from_str(r#"{"customer":42,"month":"2026-07"}"#).expect("parameters");
        let a = first
            .input_fingerprint("blueprint-a", &left)
            .expect("fingerprint");
        let b = first
            .input_fingerprint("blueprint-a", &right)
            .expect("fingerprint");
        assert_eq!(a, b);
        assert_ne!(
            a,
            second
                .input_fingerprint("blueprint-a", &left)
                .expect("fingerprint")
        );
        assert!(!a.contains("2026-07"));
    }
}
