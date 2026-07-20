use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
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
}
