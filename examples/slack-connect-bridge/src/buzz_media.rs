//! Blossom upload/download against our own Buzz relay, signed as the bridge.

use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use bytes::Bytes;
use nostr::{EventBuilder, JsonUtil, Keys, Kind, Tag, Timestamp};
use reqwest::Url;
use sha2::{Digest, Sha256};

use crate::media::{
    buzz_media_url_allowed, read_capped, relay_http_origin, transfer_timeout, BlobDescriptor,
    CopyFailure,
};

pub(crate) struct BuzzMedia {
    http: reqwest::Client,
    origin: Url,
    server: String,
    keys: Keys,
}

impl BuzzMedia {
    pub(crate) fn new(relay_ws_url: &str, keys: Keys) -> Result<Self> {
        let origin = relay_http_origin(relay_ws_url)?;
        let host = origin.host_str().context("relay URL has no host")?;
        let server = match origin.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        };
        Ok(Self {
            http: reqwest::Client::builder().build()?,
            origin,
            server,
            keys,
        })
    }

    pub(crate) async fn upload(
        &self,
        body: Bytes,
        mime: &str,
    ) -> Result<BlobDescriptor, CopyFailure> {
        let sha256 = hex::encode(Sha256::digest(&body));
        let auth = blossom_auth(&self.keys, "upload", Some(&sha256), &self.server)
            .map_err(|_| CopyFailure::Failed)?;
        let url = self
            .origin
            .join("upload")
            .map_err(|_| CopyFailure::Failed)?;
        let response = self
            .http
            .put(url)
            .timeout(transfer_timeout(mime))
            .header("Authorization", auth)
            .header("Content-Type", mime)
            .header("X-SHA-256", &sha256)
            .body(body)
            .send()
            .await
            .map_err(|_| CopyFailure::Failed)?;
        if !response.status().is_success() {
            tracing::warn!(status = %response.status(), %mime, "Buzz rejected bridged file upload");
            return Err(CopyFailure::Failed);
        }
        response
            .json::<BlobDescriptor>()
            .await
            .map_err(|_| CopyFailure::Failed)
    }

    pub(crate) async fn download(
        &self,
        url: &str,
        mime: &str,
        cap: u64,
    ) -> Result<Bytes, CopyFailure> {
        if !buzz_media_url_allowed(url, &self.origin) {
            return Err(CopyFailure::NotAllowed);
        }
        let auth =
            blossom_auth(&self.keys, "get", None, &self.server).map_err(|_| CopyFailure::Failed)?;
        let response = self
            .http
            .get(url)
            .timeout(transfer_timeout(mime))
            .header("Authorization", auth)
            .send()
            .await
            .map_err(|_| CopyFailure::Failed)?;
        read_capped(response, cap).await
    }
}

/// kind-24242 Blossom auth header, same tags as buzz-cli's upload/get signers.
fn blossom_auth(keys: &Keys, action: &str, sha256: Option<&str>, server: &str) -> Result<String> {
    let expiration = (Timestamp::now().as_secs() + 60).to_string();
    let mut tags = vec![Tag::parse(["t", action])?];
    if let Some(sha256) = sha256 {
        tags.push(Tag::parse(["x", sha256])?);
    }
    tags.push(Tag::parse(["expiration", expiration.as_str()])?);
    tags.push(Tag::parse(["server", server])?);
    let event = EventBuilder::new(Kind::from(24242), format!("Bridge {action}"))
        .tags(tags)
        .sign_with_keys(keys)?;
    Ok(format!(
        "Nostr {}",
        URL_SAFE_NO_PAD.encode(event.as_json().as_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::Request,
        http::StatusCode,
        routing::{get, put},
        Json, Router,
    };

    async fn fake_buzz() -> (String, Keys) {
        async fn upload(req: Request) -> Result<Json<serde_json::Value>, StatusCode> {
            let header = |name: &str| {
                req.headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            let (auth, sha) = (header("authorization"), header("x-sha-256"));
            if !auth.starts_with("Nostr ") || sha.len() != 64 {
                return Err(StatusCode::UNAUTHORIZED);
            }
            Ok(Json(serde_json::json!({
                "url": format!("https://buzz.example.test/media/{sha}.png"),
                "sha256": sha, "size": 3, "type": "image/png", "uploaded": 1
            })))
        }
        async fn media(req: Request) -> Result<Vec<u8>, StatusCode> {
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if auth.starts_with("Nostr ") {
                Ok(b"abc".to_vec())
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
        let app = Router::new()
            .route("/upload", put(upload))
            .route("/media/{name}", get(media));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("ws://{addr}"), Keys::generate())
    }

    #[tokio::test]
    async fn upload_signs_and_returns_descriptor() {
        let (relay, keys) = fake_buzz().await;
        let media = BuzzMedia::new(&relay, keys).unwrap();
        let desc = media
            .upload(Bytes::from_static(b"abc"), "image/png")
            .await
            .unwrap();
        assert_eq!(desc.mime_type, "image/png");
        assert_eq!(desc.sha256.len(), 64);
    }

    #[tokio::test]
    async fn download_only_from_relay_origin() {
        let (relay, keys) = fake_buzz().await;
        let media = BuzzMedia::new(&relay, keys).unwrap();
        let own = format!("{}media/aa.png", relay_http_origin(&relay).unwrap());
        assert_eq!(
            media.download(&own, "image/png", 10).await.unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(
            media
                .download("https://evil.example/x.png", "image/png", 10)
                .await,
            Err(CopyFailure::NotAllowed)
        );
        assert_eq!(
            media.download(&own, "image/png", 2).await,
            Err(CopyFailure::TooLarge)
        );
    }

    #[test]
    fn blossom_auth_carries_the_expected_tags() {
        let keys = Keys::generate();
        let header =
            blossom_auth(&keys, "upload", Some(&"a".repeat(64)), "buzz.example.test").unwrap();
        let json = String::from_utf8(
            URL_SAFE_NO_PAD
                .decode(header.strip_prefix("Nostr ").unwrap())
                .unwrap(),
        )
        .unwrap();
        assert!(json.contains(r#"["t","upload"]"#));
        assert!(json.contains(r#"["server","buzz.example.test"]"#));
        assert!(json.contains(&format!(r#"["x","{}"]"#, "a".repeat(64))));
    }
}
