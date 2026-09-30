use std::{collections::HashMap, time::Duration};

use anyhow::{bail, Context, Result};
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};

const SLACK_API_ORIGIN: &str = "https://slack.com/api";
const MAX_API_ATTEMPTS: usize = 3;
const MAX_RETRY_AFTER_SECS: u64 = 60;
const MAX_SLACK_TEXT_CHARS: usize = 39_000;

pub(crate) struct SlackClient {
    http: reqwest::Client,
    bot_token: String,
    api_origin: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SlackIdentity {
    pub(crate) team_id: String,
    pub(crate) user_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SlackConversation {
    #[serde(default)]
    pub(crate) is_ext_shared: bool,
    #[serde(default)]
    pub(crate) is_private: bool,
    #[serde(default)]
    pub(crate) is_archived: bool,
    #[serde(default)]
    pub(crate) name: String,
}

/// One file to upload to Slack.
pub(crate) struct UploadFile {
    pub(crate) name: String,
    pub(crate) mime: String,
    pub(crate) body: bytes::Bytes,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SlackPostedMessage {
    pub(crate) ts: String,
}

#[derive(Deserialize)]
struct ApiEnvelope {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(flatten)]
    rest: HashMap<String, Value>,
}

impl SlackClient {
    pub(crate) fn new(bot_token: String) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("buzz-slack-connect-bridge/0.1")
            .build()
            .context("failed to build Slack HTTP client")?;
        Ok(Self {
            http,
            bot_token,
            api_origin: SLACK_API_ORIGIN.to_owned(),
        })
    }

    pub(crate) async fn auth_test(&self) -> Result<SlackIdentity> {
        let value = self.call(Method::POST, "auth.test", None).await?;
        Ok(SlackIdentity {
            team_id: required_string(&value, "team_id", "auth.test")?,
            user_id: required_string(&value, "user_id", "auth.test")?,
        })
    }

    pub(crate) async fn conversation_info(&self, channel_id: &str) -> Result<SlackConversation> {
        let value = self
            .call_read("conversations.info", &[("channel", channel_id)])
            .await?;
        serde_json::from_value(
            value
                .get("channel")
                .cloned()
                .context("Slack conversations.info response omitted channel")?,
        )
        .context("invalid channel in Slack conversations.info response")
    }

    pub(crate) async fn user_display_name(&self, user_id: &str) -> Result<String> {
        let value = self.call_read("users.info", &[("user", user_id)]).await?;
        let user = value
            .get("user")
            .context("Slack users.info response omitted user")?;
        let profile = user
            .get("profile")
            .context("Slack users.info response omitted profile")?;
        for field in ["display_name", "real_name"] {
            if let Some(name) = profile.get(field).and_then(Value::as_str) {
                if !name.trim().is_empty() {
                    return Ok(name.trim().to_owned());
                }
            }
        }
        Ok(user
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .unwrap_or(user_id)
            .to_owned())
    }

    pub(crate) async fn post_message(
        &self,
        channel_id: &str,
        text: &str,
        thread_ts: Option<&str>,
        client_msg_id: &str,
    ) -> Result<SlackPostedMessage> {
        let mut payload = json!({
            "channel": channel_id,
            "text": truncate_slack_text(text),
            "client_msg_id": client_msg_id,
            "unfurl_links": false,
            "unfurl_media": false
        });
        if let Some(thread_ts) = thread_ts {
            payload["thread_ts"] = Value::String(thread_ts.to_owned());
        }
        let value = self
            .call(Method::POST, "chat.postMessage", Some(payload))
            .await?;
        Ok(SlackPostedMessage {
            ts: required_string(&value, "ts", "chat.postMessage")?,
        })
    }

    pub(crate) async fn download_file(
        &self,
        url: &str,
        mime: &str,
        cap: u64,
    ) -> Result<bytes::Bytes, crate::media::CopyFailure> {
        use crate::media::{read_capped, slack_download_allowed, transfer_timeout, CopyFailure};
        if !slack_download_allowed(url) {
            return Err(CopyFailure::NotAllowed);
        }
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.bot_token)
            .timeout(transfer_timeout(mime))
            .send()
            .await
            .map_err(|_| CopyFailure::Failed)?;
        read_capped(response, cap).await
    }

    /// Upload files, share them into the channel (and thread) with `comment`,
    /// and return the resulting message ts if Slack reports it within ~10 s.
    /// Callers pass at least one file.
    pub(crate) async fn upload_files(
        &self,
        channel_id: &str,
        thread_ts: Option<&str>,
        comment: &str,
        files: Vec<UploadFile>,
    ) -> Result<Option<String>> {
        let mut ids = Vec::new();
        for file in files {
            let length = file.body.len().to_string();
            let value = self
                .send(
                    Method::GET,
                    "files.getUploadURLExternal",
                    None,
                    &[
                        ("filename", file.name.as_str()),
                        ("length", length.as_str()),
                    ],
                )
                .await?;
            let upload_url = required_string(&value, "upload_url", "files.getUploadURLExternal")?;
            let file_id = required_string(&value, "file_id", "files.getUploadURLExternal")?;
            let response = self
                .http
                .post(&upload_url)
                .timeout(crate::media::transfer_timeout(&file.mime))
                .body(file.body)
                .send()
                .await
                .context("Slack file upload request failed")?;
            if !response.status().is_success() {
                bail!("Slack file upload returned HTTP {}", response.status());
            }
            ids.push(json!({ "id": file_id, "title": file.name }));
        }
        let first_id = ids
            .first()
            .and_then(|file| file["id"].as_str())
            .context("upload_files called without files")?
            .to_owned();
        let files_json = Value::Array(ids).to_string();
        let mut query: Vec<(&str, &str)> = vec![
            ("files", files_json.as_str()),
            ("channel_id", channel_id),
            ("initial_comment", comment),
        ];
        if let Some(thread_ts) = thread_ts {
            query.push(("thread_ts", thread_ts));
        }
        self.send(Method::POST, "files.completeUploadExternal", None, &query)
            .await?;

        // Slack shares uploaded files asynchronously; look the message ts up briefly.
        for _ in 0..10 {
            let info = self
                .call_read("files.info", &[("file", first_id.as_str())])
                .await?;
            if let Some(ts) = share_ts(&info, channel_id) {
                return Ok(Some(ts));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(None)
    }

    /// Slack's read methods (`conversations.info`, `users.info`) reject JSON
    /// bodies with `invalid_arguments`; they take query/form parameters. Write
    /// methods such as `chat.postMessage` take JSON.
    async fn call_read(&self, endpoint: &str, query: &[(&str, &str)]) -> Result<Value> {
        self.send(Method::GET, endpoint, None, query).await
    }

    async fn call(&self, method: Method, endpoint: &str, payload: Option<Value>) -> Result<Value> {
        self.send(method, endpoint, payload, &[]).await
    }

    async fn send(
        &self,
        method: Method,
        endpoint: &str,
        payload: Option<Value>,
        query: &[(&str, &str)],
    ) -> Result<Value> {
        let url =
            reqwest::Url::parse_with_params(&format!("{}/{endpoint}", self.api_origin), query)
                .with_context(|| format!("invalid Slack {endpoint} URL"))?;
        let mut last_error = None;

        for attempt in 0..MAX_API_ATTEMPTS {
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .bearer_auth(&self.bot_token);
            if let Some(payload) = &payload {
                request = request.json(payload);
            }

            let response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    last_error = Some(
                        anyhow::Error::new(error)
                            .context(format!("Slack {endpoint} request failed")),
                    );
                    retry_transport(attempt).await;
                    continue;
                }
            };

            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(1)
                    .min(MAX_RETRY_AFTER_SECS);
                last_error = Some(anyhow::anyhow!("Slack {endpoint} rate limited the bridge"));
                tokio::time::sleep(Duration::from_secs(retry_after)).await;
                continue;
            }

            let status = response.status();
            let body: Value = response
                .json()
                .await
                .with_context(|| format!("Slack {endpoint} returned non-JSON HTTP {status}"))?;
            if !status.is_success() {
                last_error = Some(anyhow::anyhow!("Slack {endpoint} returned HTTP {status}"));
                if status.is_server_error() {
                    retry_transport(attempt).await;
                    continue;
                }
                break;
            }

            let envelope: ApiEnvelope = serde_json::from_value(body)
                .with_context(|| format!("invalid Slack {endpoint} response"))?;
            if !envelope.ok {
                let code = envelope.error.unwrap_or_else(|| "unknown_error".to_owned());
                bail!("Slack {endpoint} failed: {code}");
            }
            return Ok(Value::Object(envelope.rest.into_iter().collect()));
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Slack {endpoint} request failed")))
    }
}

fn share_ts(info: &Value, channel_id: &str) -> Option<String> {
    let shares = info.get("file")?.get("shares")?;
    ["private", "public"].iter().find_map(|kind| {
        shares
            .get(kind)?
            .get(channel_id)?
            .as_array()?
            .first()?
            .get("ts")?
            .as_str()
            .map(str::to_owned)
    })
}

fn required_string(value: &Value, field: &str, endpoint: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .with_context(|| format!("Slack {endpoint} response omitted {field}"))
}

async fn retry_transport(attempt: usize) {
    if attempt + 1 < MAX_API_ATTEMPTS {
        let delay_ms = 250_u64.saturating_mul(1_u64 << attempt.min(4));
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

fn truncate_slack_text(text: &str) -> String {
    if text.chars().count() <= MAX_SLACK_TEXT_CHARS {
        return text.to_owned();
    }
    let mut truncated: String = text.chars().take(MAX_SLACK_TEXT_CHARS - 16).collect();
    truncated.push_str("\n… _(truncated)_");
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_preserves_utf8_boundaries() {
        let text = "🐝".repeat(MAX_SLACK_TEXT_CHARS + 10);
        let truncated = truncate_slack_text(&text);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.chars().count() <= MAX_SLACK_TEXT_CHARS);
        assert!(truncated.ends_with("… _(truncated)_"));
    }

    /// Fake Slack read endpoint that behaves like the real one: arguments must
    /// arrive as query/form parameters; a JSON body gets `invalid_arguments`.
    async fn fake_slack_read(
        axum::extract::Query(params): axum::extract::Query<
            std::collections::HashMap<String, String>,
        >,
        headers: axum::http::HeaderMap,
    ) -> axum::Json<Value> {
        let json_body = headers
            .get(axum::http::header::CONTENT_TYPE)
            .is_some_and(|v| v.as_bytes().starts_with(b"application/json"));
        if json_body || !(params.contains_key("channel") || params.contains_key("user")) {
            return axum::Json(json!({ "ok": false, "error": "invalid_arguments" }));
        }
        axum::Json(json!({
            "ok": true,
            "channel": { "is_private": true, "name": "buzz-bridge-test" },
            "user": { "name": "ram", "profile": { "display_name": "Ram" } }
        }))
    }

    async fn fake_slack() -> SlackClient {
        let app = axum::Router::new()
            .route("/conversations.info", axum::routing::any(fake_slack_read))
            .route("/users.info", axum::routing::any(fake_slack_read));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client = SlackClient::new("xoxb-test".to_owned()).unwrap();
        client.api_origin = origin;
        client
    }

    #[tokio::test]
    async fn conversation_info_sends_arguments_slack_accepts() {
        let info = fake_slack()
            .await
            .conversation_info("C0C5B8Q1A1L")
            .await
            .unwrap();
        assert!(info.is_private);
        assert_eq!(info.name, "buzz-bridge-test");
    }

    #[tokio::test]
    async fn download_file_refuses_other_hosts() {
        let client = SlackClient::new("xoxb-test".to_owned()).unwrap();
        assert_eq!(
            client
                .download_file("https://evil.example/x.png", "image/png", 10)
                .await,
            Err(crate::media::CopyFailure::NotAllowed)
        );
    }

    #[tokio::test]
    async fn upload_files_runs_the_external_upload_flow_and_finds_the_ts() {
        use axum::extract::Query;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        type Calls = Arc<Mutex<Vec<String>>>;
        #[derive(Clone)]
        struct Fake {
            calls: Calls,
            origin: Arc<Mutex<String>>,
        }
        async fn get_url(
            axum::extract::State(f): axum::extract::State<Fake>,
            Query(q): Query<HashMap<String, String>>,
        ) -> axum::Json<Value> {
            f.calls
                .lock()
                .unwrap()
                .push(format!("get:{}:{}", q["filename"], q["length"]));
            let origin = f.origin.lock().unwrap().clone();
            axum::Json(json!({"ok": true, "upload_url": format!("{origin}/put"), "file_id": "F9"}))
        }
        async fn put_bytes(
            axum::extract::State(f): axum::extract::State<Fake>,
            body: axum::body::Bytes,
        ) -> &'static str {
            f.calls.lock().unwrap().push(format!("put:{}", body.len()));
            "OK"
        }
        async fn complete(
            axum::extract::State(f): axum::extract::State<Fake>,
            Query(q): Query<HashMap<String, String>>,
        ) -> axum::Json<Value> {
            f.calls.lock().unwrap().push(format!(
                "complete:{}:{}:{}",
                q["channel_id"],
                q.get("thread_ts").cloned().unwrap_or_default(),
                q["initial_comment"]
            ));
            axum::Json(json!({"ok": true, "files": [{"id": "F9"}]}))
        }
        async fn info(axum::extract::State(f): axum::extract::State<Fake>) -> axum::Json<Value> {
            f.calls.lock().unwrap().push("info".into());
            axum::Json(
                json!({"ok": true, "file": {"shares": {"private": {"C1": [{"ts": "1790000000.000200"}]}}}}),
            )
        }
        let fake = Fake {
            calls: Arc::default(),
            origin: Arc::default(),
        };
        let app = axum::Router::new()
            .route("/files.getUploadURLExternal", axum::routing::any(get_url))
            .route("/put", axum::routing::post(put_bytes))
            .route(
                "/files.completeUploadExternal",
                axum::routing::any(complete),
            )
            .route("/files.info", axum::routing::any(info))
            .with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        *fake.origin.lock().unwrap() = origin.clone();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut client = SlackClient::new("xoxb-test".to_owned()).unwrap();
        client.api_origin = origin;

        let ts = client
            .upload_files(
                "C1",
                Some("1790000000.000100"),
                "*ram · Buzz*\nhi",
                vec![UploadFile {
                    name: "a.png".into(),
                    mime: "image/png".into(),
                    body: bytes::Bytes::from_static(b"abc"),
                }],
            )
            .await
            .unwrap();
        assert_eq!(ts.as_deref(), Some("1790000000.000200"));
        let calls = fake.calls.lock().unwrap().clone();
        assert_eq!(calls[0], "get:a.png:3");
        assert_eq!(calls[1], "put:3");
        assert_eq!(calls[2], "complete:C1:1790000000.000100:*ram · Buzz*\nhi");
        assert_eq!(calls[3], "info");
    }

    #[tokio::test]
    async fn user_display_name_sends_arguments_slack_accepts() {
        let name = fake_slack().await.user_display_name("U123").await.unwrap();
        assert_eq!(name, "Ram");
    }
}
