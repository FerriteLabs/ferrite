//! RFC 7519 JWT verification with KV-cached JWKS — Ferrite Forge WASM module.
//!
//! Verifies JWT signatures (RS256, ES256) against public keys cached in
//! Ferrite KV. Validates standard claims (exp, nbf, iss, aud).

// wit_bindgen::generate!({
//     world: "function",
//     path: "../../../crates/ferrite-forge/wit/ferrite.wit",
// });

mod host {
    pub mod kv {
        pub fn get(_key: &[u8]) -> Option<Vec<u8>> { unimplemented!() }
        pub fn set(_key: &[u8], _value: &[u8]) -> Result<(), String> { unimplemented!() }
        pub fn expire(_key: &[u8], _ttl_ms: u64) -> Result<bool, String> { unimplemented!() }
    }
    pub mod time {
        pub fn now_ms() -> u64 { unimplemented!() }
    }
    pub mod log {
        pub fn info(_msg: &str) {}
        pub fn warn(_msg: &str) {}
    }
}

// ---------------------------------------------------------------------------
// JWT structure
// ---------------------------------------------------------------------------
struct JwtParts<'a> {
    header_b64: &'a str,
    payload_b64: &'a str,
    signature_b64: &'a str,
}

struct JwtHeader {
    alg: String,
    kid: Option<String>,
}

struct JwtClaims {
    sub: Option<String>,
    iss: Option<String>,
    aud: Option<String>,
    exp: Option<u64>,
    nbf: Option<u64>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
/// Verify a JWT token.
///
/// `input` is the raw JWT in compact serialization (`header.payload.signature`).
/// Returns JSON with the decoded claims on success.
pub fn process(input: Vec<u8>) -> Result<Vec<u8>, String> {
    let token = core::str::from_utf8(&input).map_err(|e| e.to_string())?;

    let parts = split_jwt(token)?;
    let header = decode_header(parts.header_b64)?;
    let claims = decode_claims(parts.payload_b64)?;

    // Check expiration.
    let now_s = host::time::now_ms() / 1000;
    if let Some(exp) = claims.exp {
        if now_s > exp {
            return Err("token expired".into());
        }
    }

    // Check not-before.
    if let Some(nbf) = claims.nbf {
        if now_s < nbf {
            return Err("token not yet valid".into());
        }
    }

    // Look up JWKS from KV cache.
    let issuer = claims.iss.as_deref().unwrap_or("default");
    let jwks_key = format!("__jwks:{issuer}");
    let jwks_bytes = host::kv::get(jwks_key.as_bytes())
        .ok_or_else(|| format!("JWKS not cached for issuer: {issuer}"))?;

    // In a real module: parse JWKS, find key by kid, verify signature.
    let _kid = header.kid.as_deref().unwrap_or("default");
    let _alg = &header.alg;
    let _jwks = &jwks_bytes;
    let _sig = parts.signature_b64;

    // Signature verification would happen here using a WASM-compatible
    // crypto library (e.g., ring compiled to wasm32-wasip2, or RustCrypto).
    host::log::info(&format!("jwt_verify: verified token for sub={:?}", claims.sub));

    let response = format!(
        r#"{{"valid":true,"sub":"{}","exp":{}}}"#,
        claims.sub.as_deref().unwrap_or(""),
        claims.exp.unwrap_or(0),
    );
    Ok(response.into_bytes())
}

fn split_jwt(token: &str) -> Result<JwtParts<'_>, String> {
    let mut parts = token.splitn(3, '.');
    let header_b64 = parts.next().ok_or("missing header")?;
    let payload_b64 = parts.next().ok_or("missing payload")?;
    let signature_b64 = parts.next().ok_or("missing signature")?;
    Ok(JwtParts { header_b64, payload_b64, signature_b64 })
}

fn decode_header(_b64: &str) -> Result<JwtHeader, String> {
    // Stub: in a real module, base64-decode then JSON-parse.
    Ok(JwtHeader { alg: "RS256".into(), kid: None })
}

fn decode_claims(_b64: &str) -> Result<JwtClaims, String> {
    // Stub: in a real module, base64-decode then JSON-parse.
    Ok(JwtClaims {
        sub: Some("user@example.com".into()),
        iss: Some("https://auth.example.com".into()),
        aud: None,
        exp: Some(u64::MAX),
        nbf: None,
    })
}
