//! Module metadata, ACL, and the loaded-module type held by the registry.

use serde::{Deserialize, Serialize};

/// Read/write declaration enforced by Forge before a call dispatches.
///
/// Per ADR-019 §APIs, every module declares the keyspace it touches.  The
/// router uses this for cluster-key locality; the ACL layer uses it to
/// require matching `READ`/`WRITE` permissions on the calling user.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModuleAcl {
    /// Glob patterns the module reads from (e.g. `["user:*"]`).
    pub read_keys: Vec<String>,
    /// Glob patterns the module writes to.
    pub write_keys: Vec<String>,
}

/// Side-band metadata about a registered module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleMeta {
    pub name: String,
    /// SHA-256 of the wasm bytes (lowercase hex).
    pub sha256: String,
    /// Compiled module size in bytes.
    pub size_bytes: u64,
    /// Optional COSE_Sign1 envelope as base64 (P5 signed-modules feature).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// A loaded Forge module: metadata + ACL + raw wasm bytes.
///
/// Compiled `wasmtime::Module` instances live in the per-worker engine cache —
/// not in this struct — so this type is `Send + Sync` without engine deps.
#[derive(Debug, Clone)]
pub struct Module {
    pub meta: ModuleMeta,
    pub acl: ModuleAcl,
    pub bytes: Vec<u8>,
}

impl Module {
    pub fn new(name: impl Into<String>, bytes: Vec<u8>, acl: ModuleAcl) -> Self {
        let name = name.into();
        let sha256 = hex_sha256(&bytes);
        let size_bytes = bytes.len() as u64;
        Self {
            meta: ModuleMeta {
                name,
                sha256,
                size_bytes,
                signature: None,
            },
            acl,
            bytes,
        }
    }
}

/// Wire-format envelope used by replication and `__ferrite:forge:m:*` keys.
///
/// Bincode-encoded so the bytes are stable across Ferrite versions that pin
/// the same `bincode = "1"` workspace dep.  The wasm bytes themselves are
/// embedded — replicas receiving an envelope can verify the SHA-256 against
/// `meta.sha256` before installing.  Note: the envelope flattens
/// [`ModuleMeta`]'s fields rather than nesting it, because bincode's
/// fixed-format encoding can't tolerate the `skip_serializing_if` attribute
/// on `ModuleMeta::signature`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleEnvelope {
    pub name: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub signature: Option<String>,
    pub acl: ModuleAcl,
    pub bytes: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("sha mismatch: declared {declared}, actual {actual}")]
    ShaMismatch { declared: String, actual: String },
}

impl Module {
    /// Bincode-encode this module for replication / on-disk storage.
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let env = ModuleEnvelope {
            name: self.meta.name.clone(),
            sha256: self.meta.sha256.clone(),
            size_bytes: self.meta.size_bytes,
            signature: self.meta.signature.clone(),
            acl: self.acl.clone(),
            bytes: self.bytes.clone(),
        };
        Ok(bincode::serialize(&env)?)
    }

    /// Decode an envelope and verify that the embedded sha256 matches the
    /// bytes — replicas reject tampered envelopes here, before any wasm is
    /// loaded into the engine.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let env: ModuleEnvelope = bincode::deserialize(buf)?;
        let actual = hex_sha256(&env.bytes);
        if actual != env.sha256 {
            return Err(CodecError::ShaMismatch {
                declared: env.sha256,
                actual,
            });
        }
        Ok(Self {
            meta: ModuleMeta {
                name: env.name,
                sha256: env.sha256,
                size_bytes: env.size_bytes,
                signature: env.signature,
            },
            acl: env.acl,
            bytes: env.bytes,
        })
    }
}

/// Cryptographic SHA-256 of the module bytes, lowercase hex.
fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_records_size_and_acl() {
        let acl = ModuleAcl {
            read_keys: vec!["a:*".into()],
            write_keys: vec!["b:*".into()],
        };
        let m = Module::new("m1", vec![1, 2, 3, 4], acl.clone());
        assert_eq!(m.meta.name, "m1");
        assert_eq!(m.meta.size_bytes, 4);
        assert_eq!(m.acl, acl);
        assert_eq!(m.meta.sha256.len(), 64); // SHA-256 hex
        assert!(m.meta.sha256.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn sha256_is_deterministic_and_distinguishes_input() {
        let a = Module::new("x", vec![1, 2, 3], ModuleAcl::default());
        let b = Module::new("x", vec![1, 2, 3], ModuleAcl::default());
        let c = Module::new("x", vec![1, 2, 4], ModuleAcl::default());
        assert_eq!(a.meta.sha256, b.meta.sha256);
        assert_ne!(a.meta.sha256, c.meta.sha256);
    }

    #[test]
    fn module_meta_is_serializable() {
        let m = Module::new("x", vec![0u8; 8], ModuleAcl::default());
        let json = serde_json::to_string(&m.meta).unwrap();
        let back: ModuleMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "x");
        assert_eq!(back.size_bytes, 8);
    }

    #[test]
    fn module_codec_roundtrip_preserves_sha() {
        let acl = ModuleAcl {
            read_keys: vec!["a:*".into()],
            write_keys: vec!["b:*".into()],
        };
        let m = Module::new("m1", vec![10, 20, 30, 40, 50], acl);
        let buf = m.encode().expect("encode");
        let back = Module::decode(&buf).expect("decode");
        assert_eq!(back.meta.name, m.meta.name);
        assert_eq!(back.meta.sha256, m.meta.sha256);
        assert_eq!(back.acl, m.acl);
        assert_eq!(back.bytes, m.bytes);
    }

    #[test]
    fn decode_rejects_tampered_bytes() {
        let m = Module::new("m1", vec![1, 2, 3], ModuleAcl::default());
        let mut buf = m.encode().expect("encode");
        // Flip the last bincode byte — it's part of the embedded bytes.
        let last = buf.last_mut().unwrap();
        *last = last.wrapping_add(1);
        let err = Module::decode(&buf);
        assert!(matches!(err, Err(CodecError::ShaMismatch { .. })));
    }
}
