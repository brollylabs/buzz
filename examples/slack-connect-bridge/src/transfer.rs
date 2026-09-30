//! File transfers for bridged messages. These run in spawned tasks, off the
//! bridge's event loop, so a slow upload never stalls relay pings or Slack
//! acknowledgements; the loop only publishes and records the results.

use crate::{
    bridge::compose_buzz_comment,
    buzz_media::BuzzMedia,
    media::{imeta_tag, media_markdown, BlobDescriptor, BuzzAttachment, CopyFailure},
    slack::{SlackClient, SlackFile, UploadFile},
};

/// Files beyond this in one message become "too many files" lines.
pub(crate) const MAX_ATTACHMENTS_PER_MESSAGE: usize = 10;

/// Result of copying a Slack message's files into Buzz.
#[derive(Clone, Debug, Default)]
pub(crate) struct SlackCopyResult {
    /// Markdown for copied files and fallback lines for failed ones, in order.
    pub(crate) lines: Vec<String>,
    pub(crate) media_tags: Vec<Vec<String>>,
}

pub(crate) async fn copy_slack_files(
    slack: &SlackClient,
    buzz: &BuzzMedia,
    files: &[SlackFile],
    cap: u64,
) -> SlackCopyResult {
    let mut result = SlackCopyResult::default();
    for (index, file) in files.iter().enumerate() {
        let outcome = if index >= MAX_ATTACHMENTS_PER_MESSAGE {
            Err(None)
        } else {
            copy_one_slack_file(slack, buzz, file, cap)
                .await
                .map_err(Some)
        };
        match outcome {
            Ok((desc, name)) => {
                result.lines.push(media_markdown(&desc, &name));
                result.media_tags.push(imeta_tag(&desc, &name));
                tracing::info!(name = %name, size = desc.size, mime = %desc.mime_type, "copied Slack file to Buzz");
            }
            Err(failure) => {
                let reason = failure
                    .as_ref()
                    .map_or("not copied: too many files", CopyFailure::reason);
                tracing::warn!(name = %file.name, size = file.size, %reason, "Slack file not copied");
                let link = file
                    .permalink
                    .as_deref()
                    .map(|permalink| format!(" — [open in Slack]({permalink})"))
                    .unwrap_or_default();
                result.lines.push(format!(
                    "📎 {} ({reason}){link}",
                    file.name.replace(['[', ']'], "")
                ));
            }
        }
    }
    result
}

async fn copy_one_slack_file(
    slack: &SlackClient,
    buzz: &BuzzMedia,
    file: &SlackFile,
    cap: u64,
) -> Result<(BlobDescriptor, String), CopyFailure> {
    // Slack Connect files from another organisation can arrive as a bare id.
    let file = if file.url_private_download.is_none() {
        slack
            .file_info(&file.id)
            .await
            .map_err(|_| CopyFailure::Unavailable)?
    } else {
        file.clone()
    };
    if file.size > cap {
        return Err(CopyFailure::TooLarge);
    }
    let url = file
        .url_private_download
        .as_deref()
        .ok_or(CopyFailure::Unavailable)?;
    let body = slack.download_file(url, &file.mimetype, cap).await?;
    let desc = buzz.upload(body, &file.mimetype).await?;
    Ok((desc, file.name))
}

/// Everything needed to deliver one Buzz message with attachments to Slack.
#[derive(Clone, Debug)]
pub(crate) struct BuzzDelivery {
    pub(crate) channel_id: String,
    pub(crate) thread_ts: Option<String>,
    pub(crate) comment_author: String,
    pub(crate) fallback_label: &'static str,
    /// Message text with the uploaded media's markdown already removed.
    pub(crate) body: String,
    pub(crate) attachments: Vec<BuzzAttachment>,
    pub(crate) client_msg_id: String,
}

/// Post a Buzz message and its files to Slack. Returns the Slack message ts
/// when known. Files are downloaded and uploaded one at a time, so at most one
/// file per task is held in memory.
pub(crate) async fn deliver_buzz_message(
    slack: &SlackClient,
    buzz: &BuzzMedia,
    delivery: &BuzzDelivery,
    cap: u64,
) -> anyhow::Result<Option<String>> {
    let mut seen = std::collections::HashSet::new();
    let attachments: Vec<&BuzzAttachment> = delivery
        .attachments
        .iter()
        .filter(|a| seen.insert(a.url.clone()))
        .collect();
    let mut uploaded: Vec<(String, String)> = Vec::new();
    let mut failures = Vec::new();
    for (index, attachment) in attachments.iter().enumerate() {
        let outcome = if index >= MAX_ATTACHMENTS_PER_MESSAGE {
            Err("not copied: too many files".to_owned())
        } else {
            upload_one_buzz_file(slack, buzz, attachment, cap).await
        };
        match outcome {
            Ok(file_id) => uploaded.push((file_id, attachment.name.clone())),
            Err(reason) => {
                tracing::warn!(name = %attachment.name, %reason, "Buzz file not copied");
                failures.push(format!("📎 {} ({reason}) — see Buzz", attachment.name));
            }
        }
    }

    let thread_ts = delivery.thread_ts.as_deref();
    let comment = |failures: &[String]| {
        compose_buzz_comment(
            &delivery.comment_author,
            delivery.fallback_label,
            &delivery.body,
            failures,
        )
    };
    if uploaded.is_empty() {
        let posted = slack
            .post_message(
                &delivery.channel_id,
                &comment(&failures),
                thread_ts,
                &delivery.client_msg_id,
            )
            .await?;
        return Ok(Some(posted.ts));
    }
    match slack
        .complete_upload(
            &delivery.channel_id,
            thread_ts,
            &comment(&failures),
            &uploaded,
        )
        .await
    {
        Ok(ts) => Ok(ts),
        Err(error) => {
            tracing::warn!(%error, "Slack did not accept the file share; posting text only");
            let mut all = failures;
            all.extend(
                uploaded
                    .iter()
                    .map(|(_, name)| format!("📎 {name} (couldn't copy) — see Buzz")),
            );
            let posted = slack
                .post_message(
                    &delivery.channel_id,
                    &comment(&all),
                    thread_ts,
                    &delivery.client_msg_id,
                )
                .await?;
            Ok(Some(posted.ts))
        }
    }
}

async fn upload_one_buzz_file(
    slack: &SlackClient,
    buzz: &BuzzMedia,
    attachment: &BuzzAttachment,
    cap: u64,
) -> Result<String, String> {
    if attachment.size.is_some_and(|size| size > cap) {
        return Err(CopyFailure::TooLarge.reason().to_owned());
    }
    let body = buzz
        .download(&attachment.url, &attachment.mime, cap)
        .await
        .map_err(|failure| failure.reason().to_owned())?;
    slack
        .upload_file(UploadFile {
            name: attachment.name.clone(),
            mime: attachment.mime.clone(),
            body,
        })
        .await
        .map_err(|_| CopyFailure::Failed.reason().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Query, Request, State},
        http::StatusCode,
        routing::{any, get, post, put},
        Json, Router,
    };
    use nostr::Keys;
    use serde_json::{json, Value};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    /// One local server playing both Slack (API, file downloads, upload URL)
    /// and the Buzz relay (Blossom upload and media).
    #[derive(Clone, Default)]
    struct Fake {
        origin: Arc<Mutex<String>>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    async fn fake() -> (Arc<SlackClient>, Arc<BuzzMedia>, Fake) {
        async fn slack_download(
            State(f): State<Fake>,
            axum::extract::Path(name): axum::extract::Path<String>,
        ) -> Result<Vec<u8>, StatusCode> {
            f.calls
                .lock()
                .unwrap()
                .push(format!("slack-download:{name}"));
            Ok(b"png-bytes".to_vec())
        }
        async fn files_info(
            State(f): State<Fake>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            f.calls.lock().unwrap().push(format!("info:{}", q["file"]));
            if q["file"] == "FGONE" {
                return Json(json!({"ok": false, "error": "file_not_found"}));
            }
            Json(
                json!({"ok": true, "file": {"shares": {"private": {"C1": [{"ts": "1790000000.000200"}]}}}}),
            )
        }
        async fn get_url(
            State(f): State<Fake>,
            Query(q): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            f.calls
                .lock()
                .unwrap()
                .push(format!("get-url:{}", q["filename"]));
            let origin = f.origin.lock().unwrap().clone();
            Json(
                json!({"ok": true, "upload_url": format!("{origin}/put"), "file_id": format!("F-{}", q["filename"])}),
            )
        }
        async fn put_bytes(State(f): State<Fake>, body: axum::body::Bytes) -> &'static str {
            f.calls
                .lock()
                .unwrap()
                .push(format!("slack-put:{}", body.len()));
            "OK"
        }
        async fn complete(State(f): State<Fake>, body: String) -> Json<Value> {
            let form: HashMap<String, String> = form_urlencoded::parse(body.as_bytes())
                .into_owned()
                .collect();
            f.calls
                .lock()
                .unwrap()
                .push(format!("complete:{}", form["initial_comment"]));
            Json(json!({"ok": true}))
        }
        async fn post_message(State(f): State<Fake>, Json(v): Json<Value>) -> Json<Value> {
            f.calls
                .lock()
                .unwrap()
                .push(format!("post:{}", v["text"].as_str().unwrap_or("")));
            Json(json!({"ok": true, "ts": "1790000000.000300"}))
        }
        async fn buzz_upload(State(f): State<Fake>, req: Request) -> Json<Value> {
            let sha = req
                .headers()
                .get("x-sha-256")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let mime = req
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            f.calls.lock().unwrap().push(format!("buzz-upload:{mime}"));
            let origin = f.origin.lock().unwrap().clone();
            Json(
                json!({"url": format!("{origin}/media/{sha}.png"), "sha256": sha, "size": 9, "type": mime, "uploaded": 1}),
            )
        }
        async fn buzz_media(
            State(f): State<Fake>,
            axum::extract::Path(name): axum::extract::Path<String>,
        ) -> Vec<u8> {
            f.calls
                .lock()
                .unwrap()
                .push(format!("buzz-download:{name}"));
            b"file-bytes".to_vec()
        }
        let state = Fake::default();
        let app = Router::new()
            .route("/files-pri/{name}", get(slack_download))
            .route("/files.info", any(files_info))
            .route("/files.getUploadURLExternal", any(get_url))
            .route("/put", post(put_bytes))
            .route("/files.completeUploadExternal", any(complete))
            .route("/chat.postMessage", any(post_message))
            .route("/upload", put(buzz_upload))
            .route("/media/{name}", get(buzz_media))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let origin = format!("http://{addr}");
        *state.origin.lock().unwrap() = origin.clone();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let slack = SlackClient::for_tests(origin.clone());
        let buzz = BuzzMedia::new(&format!("ws://{addr}"), Keys::generate()).unwrap();
        (Arc::new(slack), Arc::new(buzz), state)
    }

    fn slack_file(id: &str, size: u64, url: Option<&str>) -> SlackFile {
        SlackFile {
            id: id.into(),
            name: format!("{id}.png"),
            mimetype: "image/png".into(),
            size,
            url_private_download: url.map(str::to_owned),
            permalink: Some(format!("https://x.slack.com/files/{id}")),
        }
    }

    #[tokio::test]
    async fn copies_slack_files_and_isolates_failures() {
        let (slack, buzz, state) = fake().await;
        let origin = state.origin.lock().unwrap().clone();
        let files = vec![
            slack_file("FOK", 9, Some(&format!("{origin}/files-pri/ok.png"))),
            slack_file("FBIG", 5_000, Some(&format!("{origin}/files-pri/big.png"))),
            slack_file("FGONE", 9, None),
        ];
        let result = copy_slack_files(&slack, &buzz, &files, 1_000).await;
        assert_eq!(result.media_tags.len(), 1);
        assert!(
            result.lines[0].starts_with("![image]("),
            "{:?}",
            result.lines
        );
        assert_eq!(
            result.lines[1],
            "📎 FBIG.png (too large to copy) — [open in Slack](https://x.slack.com/files/FBIG)"
        );
        assert_eq!(result.lines[2], "📎 FGONE.png (not available to copy) — [open in Slack](https://x.slack.com/files/FGONE)");
        let calls = state.calls.lock().unwrap().clone();
        assert!(
            !calls.iter().any(|c| c == "slack-download:big.png"),
            "oversize file was downloaded"
        );
    }

    #[tokio::test]
    async fn caps_files_per_message() {
        let (slack, buzz, state) = fake().await;
        let origin = state.origin.lock().unwrap().clone();
        let files: Vec<SlackFile> = (0..12)
            .map(|i| {
                slack_file(
                    &format!("F{i}"),
                    9,
                    Some(&format!("{origin}/files-pri/{i}.png")),
                )
            })
            .collect();
        let result = copy_slack_files(&slack, &buzz, &files, 1_000).await;
        assert_eq!(result.media_tags.len(), MAX_ATTACHMENTS_PER_MESSAGE);
        assert_eq!(
            result
                .lines
                .iter()
                .filter(|l| l.contains("too many files"))
                .count(),
            2
        );
    }

    fn delivery(attachments: Vec<BuzzAttachment>) -> BuzzDelivery {
        BuzzDelivery {
            channel_id: "C1".into(),
            thread_ts: None,
            comment_author: "ram".into(),
            fallback_label: "",
            body: "hello".into(),
            attachments,
            client_msg_id: "evt".into(),
        }
    }

    fn attachment(url: &str, name: &str) -> BuzzAttachment {
        BuzzAttachment {
            url: url.into(),
            mime: "application/pdf".into(),
            size: Some(10),
            name: name.into(),
        }
    }

    #[tokio::test]
    async fn delivers_buzz_files_with_failures_in_the_comment() {
        let (slack, buzz, state) = fake().await;
        let origin = state.origin.lock().unwrap().clone();
        let d = delivery(vec![
            attachment(&format!("{origin}/media/aa.pdf"), "report.pdf"),
            attachment("https://evil.example/x.pdf", "evil.pdf"),
            attachment(&format!("{origin}/media/aa.pdf"), "report.pdf"),
        ]);
        let ts = deliver_buzz_message(&slack, &buzz, &d, 1_000)
            .await
            .unwrap();
        assert_eq!(ts.as_deref(), Some("1790000000.000200"));
        let calls = state.calls.lock().unwrap().clone();
        assert_eq!(
            calls
                .iter()
                .filter(|c| c.starts_with("buzz-download"))
                .count(),
            1,
            "duplicate URL fetched twice: {calls:?}"
        );
        assert!(
            !calls
                .iter()
                .any(|c| c.starts_with("buzz-download") && c.contains("x.pdf")),
            "{calls:?}"
        );
        let complete = calls.iter().find(|c| c.starts_with("complete:")).unwrap();
        assert!(complete.contains("*ram · Buzz*"));
        assert!(complete.contains("📎 evil.pdf (not copied: unexpected location) — see Buzz"));
    }

    #[tokio::test]
    async fn text_only_when_nothing_could_be_uploaded() {
        let (slack, buzz, state) = fake().await;
        let d = delivery(vec![attachment("https://evil.example/x.pdf", "evil.pdf")]);
        let ts = deliver_buzz_message(&slack, &buzz, &d, 1_000)
            .await
            .unwrap();
        assert_eq!(ts.as_deref(), Some("1790000000.000300"));
        let calls = state.calls.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .any(|c| c.starts_with("post:") && c.contains("evil.pdf")),
            "{calls:?}"
        );
    }
}
