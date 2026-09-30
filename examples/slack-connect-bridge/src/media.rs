//! Pure helpers for bridging files: host allow-lists, capped downloads,
//! NIP-92 `imeta` tags and message composition. No network state lives here.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::Url;
use serde::Deserialize;

const SLACK_FILES_HOST: &str = "files.slack.com";

/// Why a file could not be copied; shown to people in the fallback line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CopyFailure {
    TooLarge,
    NotAllowed,
    Unavailable,
    Failed,
}

impl CopyFailure {
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            CopyFailure::TooLarge => "too large to copy",
            CopyFailure::NotAllowed => "not copied: unexpected location",
            CopyFailure::Unavailable => "not available to copy",
            CopyFailure::Failed => "couldn't copy",
        }
    }
}

/// `wss://host` → `https://host/` (and `ws` → `http`), the relay's HTTP origin.
pub(crate) fn relay_http_origin(relay_ws_url: &str) -> Result<Url> {
    let mut url = Url::parse(relay_ws_url).context("invalid relay URL")?;
    let scheme = match url.scheme() {
        "wss" => "https",
        "ws" => "http",
        other => bail!("unsupported relay URL scheme {other}"),
    };
    url.set_scheme(scheme)
        .map_err(|()| anyhow::anyhow!("cannot set relay URL scheme"))?;
    url.set_path("/");
    url.set_query(None);
    Ok(url)
}

pub(crate) fn slack_download_allowed(url: &str) -> bool {
    Url::parse(url).is_ok_and(|u| u.scheme() == "https" && u.host_str() == Some(SLACK_FILES_HOST))
}

pub(crate) fn buzz_media_url_allowed(url: &str, origin: &Url) -> bool {
    Url::parse(url).is_ok_and(|u| u.origin() == origin.origin())
}

pub(crate) fn transfer_timeout(mime: &str) -> Duration {
    if mime.starts_with("video/") {
        Duration::from_secs(600)
    } else {
        Duration::from_secs(120)
    }
}

/// Read a response body into memory, failing fast once it exceeds `cap` bytes.
pub(crate) async fn read_capped(
    response: reqwest::Response,
    cap: u64,
) -> Result<Bytes, CopyFailure> {
    if !response.status().is_success() {
        return Err(CopyFailure::Failed);
    }
    if response.content_length().is_some_and(|len| len > cap) {
        return Err(CopyFailure::TooLarge);
    }
    let mut body = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| CopyFailure::Failed)?;
        if (body.len() + chunk.len()) as u64 > cap {
            return Err(CopyFailure::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// Blossom blob descriptor returned by the relay after an upload.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(crate) struct BlobDescriptor {
    pub(crate) url: String,
    pub(crate) sha256: String,
    pub(crate) size: u64,
    #[serde(rename = "type")]
    pub(crate) mime_type: String,
    #[serde(default)]
    pub(crate) dim: Option<String>,
    #[serde(default)]
    pub(crate) blurhash: Option<String>,
    #[serde(default)]
    pub(crate) thumb: Option<String>,
    #[serde(default)]
    pub(crate) duration: Option<f64>,
}

/// NIP-92 `imeta` tag, identical in shape to buzz-cli's `build_imeta_tag`.
pub(crate) fn imeta_tag(desc: &BlobDescriptor, filename: &str) -> Vec<String> {
    let mut tag = vec![
        "imeta".to_owned(),
        format!("url {}", desc.url),
        format!("m {}", desc.mime_type),
        format!("x {}", desc.sha256),
        format!("size {}", desc.size),
        format!("filename {filename}"),
    ];
    if let Some(dim) = &desc.dim {
        tag.push(format!("dim {dim}"));
    }
    if let Some(blurhash) = &desc.blurhash {
        tag.push(format!("blurhash {blurhash}"));
    }
    if let Some(thumb) = &desc.thumb {
        tag.push(format!("thumb {thumb}"));
    }
    if let Some(duration) = desc.duration {
        tag.push(format!("duration {duration}"));
    }
    tag
}

/// Markdown the Buzz desktop renders for an attachment (same as buzz-cli).
pub(crate) fn media_markdown(desc: &BlobDescriptor, name: &str) -> String {
    if desc.mime_type.starts_with("image/") {
        format!("![image]({})", desc.url)
    } else if desc.mime_type.starts_with("video/") {
        format!("![video]({})", desc.url)
    } else {
        format!("[{}]({})", name.replace(['[', ']'], ""), desc.url)
    }
}

/// An attachment on a Buzz message, read from its `imeta` tag.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BuzzAttachment {
    pub(crate) url: String,
    pub(crate) mime: String,
    pub(crate) size: Option<u64>,
    pub(crate) name: String,
}

pub(crate) fn parse_imeta(event: &nostr::Event) -> Vec<BuzzAttachment> {
    event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            if parts.first().map(String::as_str) != Some("imeta") {
                return None;
            }
            let field = |key: &str| {
                parts[1..].iter().find_map(|p| {
                    p.strip_prefix(key)
                        .and_then(|v| v.strip_prefix(' '))
                        .map(str::to_owned)
                })
            };
            let url = field("url")?;
            let name = field("filename")
                .filter(|n| !n.trim().is_empty())
                .or_else(|| {
                    Url::parse(&url)
                        .ok()
                        .and_then(|u| {
                            u.path_segments()
                                .and_then(|mut s| s.next_back().map(str::to_owned))
                        })
                        .filter(|n| !n.is_empty())
                })
                .unwrap_or_else(|| "file".to_owned());
            Some(BuzzAttachment {
                mime: field("m").unwrap_or_else(|| "application/octet-stream".to_owned()),
                size: field("size").and_then(|s| s.parse().ok()),
                name,
                url,
            })
        })
        .collect()
}

/// Remove lines that only render one of `urls` (they travel as real files).
pub(crate) fn strip_media_markdown(content: &str, urls: &[String]) -> String {
    content
        .lines()
        .filter(|line| {
            let line = line.trim();
            !urls
                .iter()
                .any(|url| line.starts_with(['!', '[']) && line.ends_with(&format!("]({url})")))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, Tag};

    #[test]
    fn relay_origin_maps_ws_schemes_to_http() {
        assert_eq!(
            relay_http_origin("wss://buzz.example.test")
                .unwrap()
                .as_str(),
            "https://buzz.example.test/"
        );
        assert_eq!(
            relay_http_origin("ws://127.0.0.1:3000").unwrap().as_str(),
            "http://127.0.0.1:3000/"
        );
        assert!(relay_http_origin("ftp://x").is_err());
    }

    #[test]
    fn slack_downloads_only_from_files_slack_com_over_https() {
        assert!(slack_download_allowed(
            "https://files.slack.com/files-pri/T1-F1/download/a.png"
        ));
        assert!(!slack_download_allowed(
            "http://files.slack.com/files-pri/a.png"
        ));
        assert!(!slack_download_allowed(
            "https://files.slack.com.evil.test/a.png"
        ));
        assert!(!slack_download_allowed(
            "https://evil.test/files.slack.com/a.png"
        ));
    }

    #[test]
    fn buzz_media_only_from_the_relay_origin() {
        let origin = relay_http_origin("wss://buzz.example.test").unwrap();
        assert!(buzz_media_url_allowed(
            "https://buzz.example.test/media/abc.png",
            &origin
        ));
        assert!(!buzz_media_url_allowed(
            "https://evil.example/x.png",
            &origin
        ));
        assert!(!buzz_media_url_allowed(
            "http://buzz.example.test/media/abc.png",
            &origin
        ));
    }

    #[test]
    fn timeouts_depend_on_media_type() {
        assert_eq!(transfer_timeout("video/mp4").as_secs(), 600);
        assert_eq!(transfer_timeout("application/pdf").as_secs(), 120);
    }

    fn desc(mime: &str) -> BlobDescriptor {
        BlobDescriptor {
            url: "https://buzz.example.test/media/aa.png".into(),
            sha256: "aa".into(),
            size: 10,
            mime_type: mime.into(),
            dim: Some("2x3".into()),
            blurhash: None,
            thumb: None,
            duration: None,
        }
    }

    #[test]
    fn imeta_tag_matches_buzz_cli_format() {
        assert_eq!(
            imeta_tag(&desc("image/png"), "shot.png"),
            vec![
                "imeta",
                "url https://buzz.example.test/media/aa.png",
                "m image/png",
                "x aa",
                "size 10",
                "filename shot.png",
                "dim 2x3",
            ]
        );
    }

    #[test]
    fn media_markdown_by_type() {
        assert_eq!(
            media_markdown(&desc("image/png"), "a.png"),
            "![image](https://buzz.example.test/media/aa.png)"
        );
        assert_eq!(
            media_markdown(&desc("video/mp4"), "v.mp4"),
            "![video](https://buzz.example.test/media/aa.png)"
        );
        assert_eq!(
            media_markdown(&desc("application/pdf"), "r.pdf"),
            "[r.pdf](https://buzz.example.test/media/aa.png)"
        );
    }

    #[test]
    fn parses_imeta_from_a_buzz_event() {
        let event = EventBuilder::new(Kind::Custom(9), "hi")
            .tags(vec![Tag::parse([
                "imeta",
                "url https://buzz.example.test/media/aa.pdf",
                "m application/pdf",
                "size 7",
            ])
            .unwrap()])
            .sign_with_keys(&Keys::generate())
            .unwrap();
        assert_eq!(
            parse_imeta(&event),
            vec![BuzzAttachment {
                url: "https://buzz.example.test/media/aa.pdf".into(),
                mime: "application/pdf".into(),
                size: Some(7),
                name: "aa.pdf".into(),
            }]
        );
    }

    #[test]
    fn parse_imeta_prefers_the_filename_field() {
        let event = EventBuilder::new(Kind::Custom(9), "hi")
            .tags(vec![Tag::parse([
                "imeta",
                "url https://buzz.example.test/media/3fa9.pdf",
                "m application/pdf",
                "filename Q3 report.pdf",
            ])
            .unwrap()])
            .sign_with_keys(&Keys::generate())
            .unwrap();
        assert_eq!(parse_imeta(&event)[0].name, "Q3 report.pdf");
    }

    #[test]
    fn strips_markdown_for_uploaded_media_only() {
        let content = "look\n![image](https://b/media/aa.png)\n[doc](https://other/x)";
        assert_eq!(
            strip_media_markdown(content, &["https://b/media/aa.png".into()]),
            "look\n[doc](https://other/x)"
        );
    }

    #[tokio::test]
    async fn read_capped_stops_over_the_cap() {
        let app =
            axum::Router::new().route("/big", axum::routing::get(|| async { vec![0u8; 2048] }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = reqwest::get(format!("http://{addr}/big")).await.unwrap();
        assert_eq!(
            read_capped(response, 1024).await,
            Err(CopyFailure::TooLarge)
        );
        let response = reqwest::get(format!("http://{addr}/big")).await.unwrap();
        assert_eq!(read_capped(response, 4096).await.unwrap().len(), 2048);
    }
}
