//! Live Slack Connect ↔ Buzz message bridge.

use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use buzz_ws_client::{NostrWsConnection, RelayMessage, WsClientError};
use nostr::{Event, EventBuilder, EventId, FromBech32, Keys, Kind, Tag, Timestamp};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    buzz_media::BuzzMedia,
    config::{ChannelMapping, Config},
    media::{parse_imeta, strip_media_markdown},
    slack::{
        escape_markdown_label, slack_mrkdwn_to_markdown, slack_user_mentions, SlackClient,
        SlackDelivery, SlackEvent, SlackFile, WebhookControl,
    },
    state::{SlackMessageRef, StateStore},
    transfer::{copy_slack_files, deliver_buzz_message, BuzzDelivery, SlackCopyResult},
};

const SUBSCRIPTION_ID: &str = "slack-connect-bridge";
const BRIDGE_NAME: &str = "slack-connect-bridge";
const BRIDGE_ABOUT: &str =
    "Bridges explicitly mapped Buzz channels and Slack Connect channels without impersonating users.";
const BRIDGE_ICON_DATA_URL: &str = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 128 128'%3E%3Crect width='128' height='128' rx='28' fill='%23131622'/%3E%3Cpath d='M36 64h56M64 36v56' stroke='%237dd3fc' stroke-width='13' stroke-linecap='round'/%3E%3Ccircle cx='36' cy='64' r='13' fill='%23facc15'/%3E%3Ccircle cx='92' cy='64' r='13' fill='%23a78bfa'/%3E%3C/svg%3E";
const RECONNECT_MAX_SECS: u64 = 30;
const RELAY_POLL_TIMEOUT: Duration = Duration::from_secs(1);
const PROFILE_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct Bridge {
    config: Config,
    state: StateStore,
    slack: Arc<SlackClient>,
    buzz_media: Arc<BuzzMedia>,
    transfer_permits: Arc<Semaphore>,
    transfers_tx: mpsc::UnboundedSender<TransferDone>,
    transfers_rx: mpsc::UnboundedReceiver<TransferDone>,
    in_flight: HashSet<String>,
    slack_bot_user_id: String,
    delivery_rx: mpsc::Receiver<SlackDelivery>,
    webhook: WebhookControl,
    slack_user_names: HashMap<String, String>,
    buzz_user_names: HashMap<String, String>,
    profile_subscription_sequence: AtomicU64,
}

enum SessionOutcome {
    Reconnect(anyhow::Error),
    Shutdown,
}

struct SlackMessageInput<'a> {
    event_id: &'a str,
    team_id: &'a str,
    channel_id: &'a str,
    user_id: &'a str,
    text: &'a str,
    ts: &'a str,
    thread_ts: Option<&'a str>,
    is_ext_shared: Option<bool>,
    files: &'a [SlackFile],
}

struct SlackOriginInput<'a> {
    buzz_channel_id: Uuid,
    content: &'a str,
    team_id: &'a str,
    channel_id: &'a str,
    slack_ts: &'a str,
    user_id: &'a str,
    thread_ts: Option<&'a str>,
    reply_to: Option<EventId>,
    media_tags: &'a [Vec<String>],
}

/// Mentions resolved to names per message; the rest keep their raw IDs.
const MAX_RESOLVED_MENTIONS: usize = 20;
/// Retries for publishing a copied Slack message after a relay error.
const MAX_PUBLISH_ATTEMPTS: u8 = 3;
/// At most this many file transfers run at once (spec: bounded work).
const MAX_CONCURRENT_TRANSFERS: usize = 2;

/// A Slack message whose files are being copied off the event loop.
struct PendingSlackMessage {
    key: String,
    event_id: String,
    team_id: String,
    channel_id: String,
    user_id: String,
    text: String,
    ts: String,
    thread_ts: Option<String>,
    author: String,
    reply_to: Option<EventId>,
    fallback_label: &'static str,
    mention_names: HashMap<String, String>,
    buzz_channel_id: Uuid,
    attempts: u8,
}

/// Where a Buzz message with files was delivered, for recording the result.
struct BuzzTransferDone {
    key: String,
    buzz_event_id: String,
    buzz_channel_id: Uuid,
    team_id: String,
    channel_id: String,
    thread_ts: Option<String>,
}

enum TransferDone {
    Slack {
        pending: Box<PendingSlackMessage>,
        result: SlackCopyResult,
    },
    Buzz {
        done: BuzzTransferDone,
        result: std::result::Result<Option<String>, String>,
    },
}

impl Bridge {
    pub(crate) async fn initialize(
        config: Config,
        delivery_rx: mpsc::Receiver<SlackDelivery>,
        webhook: WebhookControl,
    ) -> Result<Self> {
        let state = StateStore::load(config.state_path.clone())?;
        let slack = SlackClient::new(config.slack_bot_token.clone())?;
        let identity = slack.auth_test().await?;
        validate_installation(&config, &identity.team_id)?;
        let buzz_media = BuzzMedia::new(&config.relay_url, config.bridge_keys.clone())?;
        let (transfers_tx, transfers_rx) = mpsc::unbounded_channel();

        let mut bridge = Self {
            config,
            state,
            slack: Arc::new(slack),
            buzz_media: Arc::new(buzz_media),
            transfer_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_TRANSFERS)),
            transfers_tx,
            transfers_rx,
            in_flight: HashSet::new(),
            slack_bot_user_id: identity.user_id,
            delivery_rx,
            webhook,
            slack_user_names: HashMap::new(),
            buzz_user_names: HashMap::new(),
            profile_subscription_sequence: AtomicU64::new(0),
        };
        bridge.validate_slack_routes().await?;
        Ok(bridge)
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        let mut reconnect_delay = 1_u64;
        loop {
            let session_started = tokio::time::Instant::now();
            let outcome = tokio::select! {
                result = self.run_session() => match result {
                    Ok(()) => SessionOutcome::Reconnect(anyhow::anyhow!("Buzz relay session ended")),
                    Err(error) => SessionOutcome::Reconnect(error),
                },
                signal = tokio::signal::ctrl_c() => {
                    signal.context("failed to listen for shutdown signal")?;
                    SessionOutcome::Shutdown
                }
            };
            self.webhook.set_ready(false);
            if session_started.elapsed() >= Duration::from_secs(RECONNECT_MAX_SECS) {
                reconnect_delay = 1;
            }

            match outcome {
                SessionOutcome::Shutdown => {
                    info!("Slack Connect bridge shutting down");
                    return Ok(());
                }
                SessionOutcome::Reconnect(error) => {
                    error!(reason = %error, reconnect_delay, "Buzz relay session failed");
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(reconnect_delay)) => {}
                signal = tokio::signal::ctrl_c() => {
                    signal.context("failed to listen for shutdown signal")?;
                    info!("Slack Connect bridge shutting down");
                    return Ok(());
                }
            }
            reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_SECS);
        }
    }

    async fn run_session(&mut self) -> Result<()> {
        let mut connection = self.connect_buzz().await?;
        self.webhook.set_ready(true);
        info!(
            routes = self.config.channels.len(),
            "Slack Connect bridge is ready"
        );

        loop {
            tokio::select! {
                delivery = self.delivery_rx.recv() => {
                    let Some(delivery) = delivery else {
                        bail!("Slack delivery queue closed");
                    };
                    let result = self
                        .process_slack_event(&mut connection, delivery.event)
                        .await;
                    let completion = result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| error.to_string());
                    let _ = delivery.completion.send(completion);
                    if let Err(error) = result {
                        warn!(reason = %error, "Slack event was not bridged");
                    }
                }
                Some(done) = self.transfers_rx.recv() => {
                    self.handle_transfer_done(&mut connection, done).await?;
                }
                relay_message = connection.next_event(RELAY_POLL_TIMEOUT) => {
                    match relay_message {
                        Ok(RelayMessage::Event { subscription_id, event })
                            if subscription_id == SUBSCRIPTION_ID =>
                        {
                            self.process_buzz_event(&event).await?;
                            self.state.record_buzz_cursor(event.created_at.as_secs())?;
                        }
                        Ok(RelayMessage::Closed { subscription_id, message })
                            if subscription_id == SUBSCRIPTION_ID =>
                        {
                            bail!("Buzz relay closed bridge subscription: {message}");
                        }
                        Ok(RelayMessage::Notice { message }) => {
                            warn!(%message, "Buzz relay notice");
                        }
                        Ok(_) => {}
                        Err(WsClientError::Timeout) => {}
                        Err(error) => return Err(error).context("Buzz relay receive failed"),
                    }
                }
            }
        }
    }

    async fn connect_buzz(&mut self) -> Result<NostrWsConnection> {
        let mut connection = NostrWsConnection::connect_authenticated(
            &self.config.relay_url,
            &self.config.bridge_keys,
            self.config.owner_auth_tag.as_ref(),
        )
        .await
        .context("failed to connect and authenticate to Buzz relay")?;

        self.publish_bridge_profile(&mut connection).await?;
        for route in &self.config.channels {
            self.announce_channel_membership(&mut connection, route)
                .await;
        }

        let channels: Vec<String> = self
            .config
            .channels
            .iter()
            .map(|route| route.buzz_channel_id.to_string())
            .collect();
        let since = self
            .state
            .subscription_since(Timestamp::now().as_secs(), self.config.replay_lookback_secs)?;
        connection
            .send_raw(&json!([
                "REQ",
                SUBSCRIPTION_ID,
                {
                    "kinds": [
                        buzz_sdk::kind::KIND_STREAM_MESSAGE,
                        buzz_sdk::kind::KIND_STREAM_MESSAGE_V2
                    ],
                    "#h": channels,
                    "since": since
                }
            ]))
            .await
            .context("failed to subscribe to mapped Buzz channels")?;
        Ok(connection)
    }

    async fn publish_bridge_profile(&self, connection: &mut NostrWsConnection) -> Result<()> {
        let event = buzz_sdk::build_profile(
            Some(self.config.display_name.as_str()),
            Some(BRIDGE_NAME),
            Some(BRIDGE_ICON_DATA_URL),
            Some(BRIDGE_ABOUT),
            None,
        )?
        .sign_with_keys(&self.config.bridge_keys)?;
        send_event_checked(connection, event)
            .await
            .context("failed to publish bridge profile")
    }

    async fn announce_channel_membership(
        &self,
        connection: &mut NostrWsConnection,
        route: &ChannelMapping,
    ) {
        let event = build_membership_event(route.buzz_channel_id, &self.config.bridge_keys);
        let result = match event {
            Ok(event) => send_event_checked(connection, event).await,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            warn!(
                buzz_channel_id = %route.buzz_channel_id,
                reason = %error,
                "bridge could not self-add as channel bot; private channels require an owner/admin to add the bridge pubkey"
            );
        }
    }

    async fn process_slack_event(
        &mut self,
        connection: &mut NostrWsConnection,
        event: SlackEvent,
    ) -> Result<()> {
        match event {
            SlackEvent::Message {
                event_id,
                team_id,
                channel_id,
                user_id,
                text,
                ts,
                thread_ts,
                is_ext_shared,
                files,
            } => {
                self.bridge_slack_message(
                    connection,
                    SlackMessageInput {
                        event_id: &event_id,
                        team_id: &team_id,
                        channel_id: &channel_id,
                        user_id: &user_id,
                        text: &text,
                        ts: &ts,
                        thread_ts: thread_ts.as_deref(),
                        is_ext_shared,
                        files: &files,
                    },
                )
                .await
            }
            SlackEvent::ChannelIdChanged {
                event_id,
                team_id,
                old_channel_id,
                new_channel_id,
            } => {
                if self
                    .route_for_slack(&team_id, &old_channel_id)
                    .or_else(|| self.route_for_slack(&team_id, &new_channel_id))
                    .is_some()
                {
                    self.state.record_channel_id_change(
                        &team_id,
                        &old_channel_id,
                        &new_channel_id,
                    )?;
                    info!(
                        %event_id,
                        %team_id,
                        %old_channel_id,
                        %new_channel_id,
                        "updated mapped Slack channel ID"
                    );
                }
                Ok(())
            }
            SlackEvent::ChannelShared {
                event_id,
                team_id,
                channel_id,
            } => {
                if let Some(route) = self.route_for_slack(&team_id, &channel_id).cloned() {
                    self.state.set_route_paused(route.buzz_channel_id, false)?;
                    info!(
                        %event_id,
                        %team_id,
                        %channel_id,
                        buzz_channel_id = %route.buzz_channel_id,
                        "resumed shared-channel route"
                    );
                }
                Ok(())
            }
            SlackEvent::ChannelUnshared {
                event_id,
                team_id,
                channel_id,
                is_ext_shared,
            } => {
                if let Some(route) = self.route_for_slack(&team_id, &channel_id).cloned() {
                    if is_ext_shared {
                        info!(
                            %event_id,
                            %team_id,
                            %channel_id,
                            buzz_channel_id = %route.buzz_channel_id,
                            "one organization left the Slack Connect channel; route remains shared"
                        );
                    } else {
                        self.state.set_route_paused(route.buzz_channel_id, true)?;
                        warn!(
                            %event_id,
                            %team_id,
                            %channel_id,
                            buzz_channel_id = %route.buzz_channel_id,
                            "paused route because Slack reported channel_unshared"
                        );
                    }
                }
                Ok(())
            }
        }
    }

    async fn bridge_slack_message(
        &mut self,
        connection: &mut NostrWsConnection,
        input: SlackMessageInput<'_>,
    ) -> Result<()> {
        let SlackMessageInput {
            event_id,
            team_id,
            channel_id,
            user_id,
            text,
            ts,
            thread_ts,
            is_ext_shared,
            files,
        } = input;
        if user_id == self.slack_bot_user_id {
            return Ok(());
        }
        if is_ext_shared == Some(false) && !self.config.allow_non_shared_channels {
            warn!(
                %event_id,
                %team_id,
                %channel_id,
                "ignored message explicitly marked as non-shared"
            );
            return Ok(());
        }

        let Some(route) = self.route_for_slack(team_id, channel_id).cloned() else {
            return Ok(());
        };
        if self.state.route_is_paused(route.buzz_channel_id) {
            warn!(
                %event_id,
                buzz_channel_id = %route.buzz_channel_id,
                "ignored message for paused Slack Connect route"
            );
            return Ok(());
        }
        let key = format!("slack:{}:{ts}", route.buzz_channel_id);
        if self
            .state
            .buzz_event_for_slack(route.buzz_channel_id, ts)
            .is_some()
            || self.in_flight.contains(&key)
        {
            return Ok(());
        }
        if text.trim().is_empty() && files.is_empty() {
            info!(%event_id, "ignored Slack message without text or files");
            return Ok(());
        }

        let author = self.slack_display_name(user_id).await?;
        let mut mention_names = HashMap::new();
        for mentioned in slack_user_mentions(text)
            .into_iter()
            .take(MAX_RESOLVED_MENTIONS)
        {
            let name = self.slack_display_name(&mentioned).await?;
            mention_names.insert(mentioned, name);
        }
        let reply_to = thread_ts
            .filter(|root_ts| *root_ts != ts)
            .and_then(|root_ts| {
                self.state
                    .buzz_event_for_slack(route.buzz_channel_id, root_ts)
            })
            .map(EventId::from_hex)
            .transpose()
            .context("bridge state contains an invalid Buzz event ID")?;
        let thread_fallback = thread_ts
            .filter(|root_ts| *root_ts != ts)
            .is_some_and(|_| reply_to.is_none());
        let fallback_label = if thread_fallback {
            "↳ _Slack thread root was not bridged; showing this reply at channel level._\n\n"
        } else {
            ""
        };
        let pending = PendingSlackMessage {
            key,
            event_id: event_id.to_owned(),
            team_id: team_id.to_owned(),
            channel_id: channel_id.to_owned(),
            user_id: user_id.to_owned(),
            text: text.to_owned(),
            ts: ts.to_owned(),
            thread_ts: thread_ts.map(str::to_owned),
            author,
            reply_to,
            fallback_label,
            mention_names,
            buzz_channel_id: route.buzz_channel_id,
            attempts: 0,
        };
        if files.is_empty() {
            return self
                .publish_slack_message(connection, &pending, &SlackCopyResult::default())
                .await;
        }

        // Files are copied off the event loop; the result comes back through
        // `transfers_rx` and is published by `handle_transfer_done`.
        self.in_flight.insert(pending.key.clone());
        let (slack, buzz, permits, done_tx) = (
            Arc::clone(&self.slack),
            Arc::clone(&self.buzz_media),
            Arc::clone(&self.transfer_permits),
            self.transfers_tx.clone(),
        );
        let files = files.to_vec();
        let cap = self.config.max_file_bytes;
        tokio::spawn(async move {
            let _permit = permits.acquire_owned().await;
            let result = copy_slack_files(&slack, &buzz, &files, cap).await;
            let _ = done_tx.send(TransferDone::Slack {
                pending: Box::new(pending),
                result,
            });
        });
        info!(%event_id, "queued Slack files for copying");
        Ok(())
    }

    async fn publish_slack_message(
        &mut self,
        connection: &mut NostrWsConnection,
        pending: &PendingSlackMessage,
        copied: &SlackCopyResult,
    ) -> Result<()> {
        let content = compose_slack_origin_content(
            &pending.author,
            pending.fallback_label,
            &pending.text,
            &copied.lines,
            &pending.mention_names,
        );
        let event = build_slack_origin_event(
            &self.config.bridge_keys,
            SlackOriginInput {
                buzz_channel_id: pending.buzz_channel_id,
                content: &content,
                team_id: &pending.team_id,
                channel_id: &pending.channel_id,
                slack_ts: &pending.ts,
                user_id: &pending.user_id,
                thread_ts: pending.thread_ts.as_deref(),
                reply_to: pending.reply_to,
                media_tags: &copied.media_tags,
            },
        )?;
        let buzz_event_id = event.id.to_hex();
        send_event_checked(connection, event).await?;

        let canonical_channel = self
            .state
            .canonical_channel_id(&pending.team_id, &pending.channel_id);
        self.state.record_message_pair(
            pending.buzz_channel_id,
            &buzz_event_id,
            SlackMessageRef {
                team_id: pending.team_id.clone(),
                channel_id: canonical_channel,
                ts: pending.ts.clone(),
                thread_ts: pending.thread_ts.clone(),
            },
        )?;
        self.in_flight.remove(&pending.key);
        info!(
            event_id = %pending.event_id,
            %buzz_event_id,
            buzz_channel_id = %pending.buzz_channel_id,
            "bridged Slack message to Buzz"
        );
        Ok(())
    }

    async fn process_buzz_event(&mut self, event: &Event) -> Result<()> {
        let event_id_hex = event.id.to_hex();
        let key = format!("buzz:{event_id_hex}");
        if event.pubkey == self.config.bridge_keys.public_key()
            || has_slack_origin(event)
            || self.state.was_delivered(&event_id_hex)
            || self.in_flight.contains(&key)
        {
            return Ok(());
        }
        let Some(buzz_channel_id) = event_channel_id(event) else {
            return Ok(());
        };
        let Some(route) = self.route_for_buzz(buzz_channel_id).cloned() else {
            return Ok(());
        };
        if self.state.route_is_paused(route.buzz_channel_id) {
            return Ok(());
        }

        let thread_root = event_thread_root(event);
        let thread_ts = thread_root.as_deref().and_then(|root_id| {
            self.state.slack_message_for_buzz(root_id).map(|reference| {
                reference
                    .thread_ts
                    .as_deref()
                    .unwrap_or(&reference.ts)
                    .to_owned()
            })
        });
        let author = self.buzz_display_name(&event.pubkey.to_hex()).await;
        let fallback_label = if thread_root.is_some() && thread_ts.is_none() {
            "↳ Buzz thread root was not bridged; showing this reply at channel level.\n\n"
        } else {
            ""
        };
        let channel_id = self
            .state
            .canonical_channel_id(&route.slack_team_id, &route.slack_channel_id);

        let mut mention_names = HashMap::new();
        for pubkey in npub_mentions(&event.content)
            .into_iter()
            .take(MAX_RESOLVED_MENTIONS)
        {
            let name = self.buzz_display_name(&pubkey).await;
            mention_names.insert(pubkey, name);
        }
        let content = replace_npub_mentions(&event.content, &mention_names);

        let attachments = parse_imeta(event);
        if attachments.is_empty() {
            let text = compose_buzz_comment(&author, fallback_label, &content, &[]);
            let posted = self
                .slack
                .post_message(&channel_id, &text, thread_ts.as_deref(), &event_id_hex)
                .await
                .with_context(|| {
                    format!("failed to post Buzz event {event_id_hex} to mapped Slack channel")
                })?;
            self.state.record_message_pair(
                route.buzz_channel_id,
                &event_id_hex,
                SlackMessageRef {
                    team_id: route.slack_team_id,
                    channel_id,
                    ts: posted.ts,
                    thread_ts,
                },
            )?;
            info!(
                buzz_event_id = %event_id_hex,
                buzz_channel_id = %route.buzz_channel_id,
                "bridged Buzz message to Slack"
            );
            return Ok(());
        }

        let urls: Vec<String> = attachments.iter().map(|a| a.url.clone()).collect();
        let delivery = BuzzDelivery {
            channel_id: channel_id.clone(),
            thread_ts: thread_ts.clone(),
            comment_author: author,
            fallback_label,
            body: strip_media_markdown(&content, &urls),
            attachments,
            client_msg_id: event_id_hex.clone(),
        };
        self.in_flight.insert(key.clone());
        let (slack, buzz, permits, done_tx) = (
            Arc::clone(&self.slack),
            Arc::clone(&self.buzz_media),
            Arc::clone(&self.transfer_permits),
            self.transfers_tx.clone(),
        );
        let cap = self.config.max_file_bytes;
        let done = BuzzTransferDone {
            key,
            buzz_event_id: event_id_hex.clone(),
            buzz_channel_id: route.buzz_channel_id,
            team_id: route.slack_team_id,
            channel_id,
            thread_ts,
        };
        tokio::spawn(async move {
            let _permit = permits.acquire_owned().await;
            let result = deliver_buzz_message(&slack, &buzz, &delivery, cap)
                .await
                .map_err(|error| error.to_string());
            let _ = done_tx.send(TransferDone::Buzz { done, result });
        });
        info!(buzz_event_id = %event_id_hex, "queued Buzz files for Slack");
        Ok(())
    }

    /// Publish or record the outcome of a background file transfer.
    async fn handle_transfer_done(
        &mut self,
        connection: &mut NostrWsConnection,
        done: TransferDone,
    ) -> Result<()> {
        match done {
            TransferDone::Slack {
                mut pending,
                result,
            } => {
                if let Err(error) = self
                    .publish_slack_message(connection, &pending, &result)
                    .await
                {
                    pending.attempts += 1;
                    if pending.attempts < MAX_PUBLISH_ATTEMPTS {
                        // Retry after the session reconnects.
                        let _ = self
                            .transfers_tx
                            .send(TransferDone::Slack { pending, result });
                    } else {
                        self.in_flight.remove(&pending.key);
                        warn!(event_id = %pending.event_id, %error, "gave up publishing Slack message with files");
                    }
                    return Err(error);
                }
                Ok(())
            }
            TransferDone::Buzz { done, result } => {
                self.in_flight.remove(&done.key);
                match result {
                    Ok(Some(ts)) => self.state.record_message_pair(
                        done.buzz_channel_id,
                        &done.buzz_event_id,
                        SlackMessageRef {
                            team_id: done.team_id,
                            channel_id: done.channel_id,
                            ts,
                            thread_ts: done.thread_ts,
                        },
                    )?,
                    Ok(None) => {
                        info!(
                            buzz_event_id = %done.buzz_event_id,
                            "Slack did not report the share's ts; replies will fall back to channel level"
                        );
                        self.state
                            .record_delivered_without_ts(&done.buzz_event_id)?;
                    }
                    Err(error) => {
                        warn!(buzz_event_id = %done.buzz_event_id, %error, "Buzz message with files not delivered; a relay replay will retry it");
                        return Ok(());
                    }
                }
                info!(buzz_event_id = %done.buzz_event_id, "bridged Buzz message with files to Slack");
                Ok(())
            }
        }
    }

    async fn slack_display_name(&mut self, user_id: &str) -> Result<String> {
        if let Some(name) = self.slack_user_names.get(user_id) {
            return Ok(name.clone());
        }
        if let Some(name) = self.state.slack_user_name(user_id) {
            let name = name.to_owned();
            self.slack_user_names
                .insert(user_id.to_owned(), name.clone());
            return Ok(name);
        }
        let name = match self.slack.user_display_name(user_id).await {
            Ok(name) => name,
            Err(error) => {
                warn!(%user_id, reason = %error, "could not resolve Slack display name");
                user_id.to_owned()
            }
        };
        self.state.record_slack_user_name(user_id, &name)?;
        self.slack_user_names
            .insert(user_id.to_owned(), name.clone());
        Ok(name)
    }

    async fn buzz_display_name(&mut self, pubkey: &str) -> String {
        if let Some(name) = self.buzz_user_names.get(pubkey) {
            return name.clone();
        }
        let fallback = abbreviated_pubkey(pubkey);
        let name = match self.query_buzz_profile(pubkey).await {
            Ok(Some(name)) => name,
            Ok(None) => fallback,
            Err(error) => {
                warn!(pubkey = %abbreviated_pubkey(pubkey), reason = %error, "could not resolve Buzz profile");
                fallback
            }
        };
        self.buzz_user_names.insert(pubkey.to_owned(), name.clone());
        name
    }

    async fn query_buzz_profile(&self, pubkey: &str) -> Result<Option<String>> {
        let mut connection = NostrWsConnection::connect_authenticated(
            &self.config.relay_url,
            &self.config.bridge_keys,
            self.config.owner_auth_tag.as_ref(),
        )
        .await?;
        let sequence = self
            .profile_subscription_sequence
            .fetch_add(1, Ordering::Relaxed);
        let subscription_id = format!("slack-profile-{sequence}");
        connection
            .send_raw(&json!([
                "REQ",
                subscription_id,
                { "kinds": [0], "authors": [pubkey], "limit": 1 }
            ]))
            .await?;

        let deadline = tokio::time::Instant::now() + PROFILE_QUERY_TIMEOUT;
        let mut result = None;
        while let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now()) {
            match connection.next_event(remaining).await {
                Ok(RelayMessage::Event {
                    subscription_id: response_id,
                    event,
                }) if response_id == subscription_id => {
                    result = profile_display_name(&event.content);
                }
                Ok(RelayMessage::Eose {
                    subscription_id: response_id,
                }) if response_id == subscription_id => break,
                Ok(RelayMessage::Closed {
                    subscription_id: response_id,
                    message,
                }) if response_id == subscription_id => {
                    bail!("Buzz profile query closed: {message}");
                }
                Ok(_) => {}
                Err(WsClientError::Timeout) => break,
                Err(error) => return Err(error.into()),
            }
        }
        let _ = connection.disconnect().await;
        Ok(result)
    }

    async fn validate_slack_routes(&mut self) -> Result<()> {
        for route in self.config.channels.clone() {
            let channel_id = self
                .state
                .canonical_channel_id(&route.slack_team_id, &route.slack_channel_id);
            let conversation = self.slack.conversation_info(&channel_id).await?;
            if conversation.is_archived {
                bail!(
                    "Slack channel {channel_id} ({}) is archived",
                    conversation.name
                );
            }
            if !conversation.is_ext_shared && !self.config.allow_non_shared_channels {
                bail!(
                    "Slack channel {channel_id} ({}) is not a Slack Connect channel; set allow_non_shared_channels only after reviewing the disclosure boundary",
                    conversation.name
                );
            }
            if conversation.is_ext_shared {
                self.state.set_route_paused(route.buzz_channel_id, false)?;
            }
            info!(
                %channel_id,
                channel_name = %conversation.name,
                is_private = conversation.is_private,
                buzz_channel_id = %route.buzz_channel_id,
                "validated Slack channel route"
            );
        }
        Ok(())
    }

    fn route_for_slack(&self, team_id: &str, channel_id: &str) -> Option<&ChannelMapping> {
        let incoming = self.state.canonical_channel_id(team_id, channel_id);
        self.config.channels.iter().find(|route| {
            route.slack_team_id == team_id
                && self
                    .state
                    .canonical_channel_id(team_id, &route.slack_channel_id)
                    == incoming
        })
    }

    fn route_for_buzz(&self, channel_id: Uuid) -> Option<&ChannelMapping> {
        self.config
            .channels
            .iter()
            .find(|route| route.buzz_channel_id == channel_id)
    }
}

fn validate_installation(config: &Config, installed_team_id: &str) -> Result<()> {
    for route in &config.channels {
        if route.slack_team_id != installed_team_id {
            bail!(
                "Slack bot token is installed in {installed_team_id}, but a route uses {}; run one bridge process per Slack installation",
                route.slack_team_id
            );
        }
    }
    Ok(())
}

fn build_membership_event(channel_id: Uuid, keys: &Keys) -> Result<Event> {
    let channel_id = channel_id.to_string();
    let pubkey = keys.public_key().to_hex();
    Ok(
        EventBuilder::new(Kind::Custom(buzz_sdk::kind::KIND_NIP29_PUT_USER as u16), "")
            .tags([
                Tag::parse(["h", channel_id.as_str()])?,
                Tag::parse(["p", pubkey.as_str()])?,
                Tag::parse(["role", "bot"])?,
            ])
            .sign_with_keys(keys)?,
    )
}

fn build_slack_origin_event(keys: &Keys, input: SlackOriginInput<'_>) -> Result<Event> {
    let SlackOriginInput {
        buzz_channel_id,
        content,
        team_id,
        channel_id,
        slack_ts,
        user_id,
        thread_ts,
        reply_to,
        media_tags,
    } = input;
    if content.len() > 64 * 1024 {
        bail!("Slack message exceeds Buzz's 64 KiB message limit");
    }
    let external_id = format!("slack:{team_id}:{channel_id}:{slack_ts}");
    let mut tags = vec![
        Tag::parse(["h", buzz_channel_id.to_string().as_str()])?,
        Tag::parse(["i", external_id.as_str()])?,
        Tag::parse(["proxy", "slack", team_id, channel_id, slack_ts, user_id])?,
        Tag::parse(["client", BRIDGE_NAME])?,
    ];
    if let Some(thread_ts) = thread_ts {
        tags.push(Tag::parse(["slack_thread_ts", thread_ts])?);
    }
    if let Some(reply_to) = reply_to {
        let reply_to = reply_to.to_hex();
        tags.push(Tag::parse(["e", reply_to.as_str(), "", "reply"])?);
    }
    for media in media_tags {
        tags.push(Tag::parse(
            media.iter().map(String::as_str).collect::<Vec<_>>(),
        )?);
    }
    let created_at = slack_timestamp(slack_ts)?;
    Ok(EventBuilder::new(
        Kind::Custom(buzz_sdk::kind::KIND_STREAM_MESSAGE as u16),
        content,
    )
    .tags(tags)
    .custom_created_at(created_at)
    .sign_with_keys(keys)?)
}

fn compose_slack_origin_content(
    author: &str,
    fallback_label: &str,
    text: &str,
    attachments: &[String],
    mention_names: &HashMap<String, String>,
) -> String {
    let mut content = format!(
        "**{} · Slack**\n{}{}",
        escape_markdown_label(author),
        fallback_label,
        slack_mrkdwn_to_markdown(text, mention_names)
    );
    for line in attachments {
        if !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(line);
    }
    content
}

/// Pubkeys (hex) of NIP-27 `nostr:npub1…` mentions in Buzz content.
fn npub_mentions(content: &str) -> Vec<String> {
    buzz_sdk::mentions::extract_nostr_uris(content)
}

/// Replace `nostr:npub1…` mentions whose pubkey is in `names` with `@name`.
/// Unknown or malformed mentions stay as they are.
fn replace_npub_mentions(content: &str, names: &HashMap<String, String>) -> String {
    const PREFIX: &str = "nostr:npub1";
    const LEN: usize = PREFIX.len() + 58;
    let mut output = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(start) = rest.find(PREFIX) {
        output.push_str(&rest[..start]);
        let candidate = rest.get(start..start + LEN);
        let name = candidate.and_then(|uri| {
            let pubkey = nostr::PublicKey::from_bech32(&uri["nostr:".len()..]).ok()?;
            names.get(&pubkey.to_hex())
        });
        match (candidate, name) {
            (Some(_), Some(name)) => {
                output.push('@');
                output.push_str(name);
                rest = &rest[start + LEN..];
            }
            _ => {
                output.push_str(PREFIX);
                rest = &rest[start + PREFIX.len()..];
            }
        }
    }
    output.push_str(rest);
    output
}

pub(crate) fn compose_buzz_comment(
    author: &str,
    fallback_label: &str,
    content: &str,
    failures: &[String],
) -> String {
    let mut text = format!(
        "*{} · Buzz*\n{}{}",
        escape_slack_label(author),
        fallback_label,
        escape_slack_message_body(content)
    );
    for line in failures {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&escape_slack_message_body(line));
    }
    text
}

async fn send_event_checked(connection: &mut NostrWsConnection, event: Event) -> Result<()> {
    let event_id = event.id.to_hex();
    let response = connection.send_event(event).await?;
    if !response.accepted {
        bail!("Buzz relay rejected event {event_id}: {}", response.message);
    }
    Ok(())
}

fn slack_timestamp(ts: &str) -> Result<Timestamp> {
    let seconds = ts
        .split_once('.')
        .map_or(ts, |(seconds, _)| seconds)
        .parse::<u64>()
        .context("Slack message has an invalid ts")?;
    Ok(Timestamp::from(seconds))
}

fn has_slack_origin(event: &Event) -> bool {
    event.tags.iter().any(|tag| {
        let parts = tag.as_slice();
        parts.first().map(String::as_str) == Some("proxy")
            && parts.get(1).map(String::as_str) == Some("slack")
    })
}

fn event_channel_id(event: &Event) -> Option<Uuid> {
    event.tags.iter().find_map(|tag| {
        let parts = tag.as_slice();
        (parts.first().map(String::as_str) == Some("h"))
            .then(|| parts.get(1))
            .flatten()
            .and_then(|value| Uuid::parse_str(value).ok())
    })
}

fn event_thread_root(event: &Event) -> Option<String> {
    let mut reply = None;
    for tag in event.tags.iter() {
        let parts = tag.as_slice();
        if parts.first().map(String::as_str) != Some("e") {
            continue;
        }
        let id = parts.get(1).filter(|id| {
            id.len() == 64 && id.chars().all(|character| character.is_ascii_hexdigit())
        });
        match (parts.get(3).map(String::as_str), id) {
            (Some("root"), Some(id)) => return Some(id.clone()),
            (Some("reply"), Some(id)) => reply = Some(id.clone()),
            _ => {}
        }
    }
    reply
}

fn profile_display_name(content: &str) -> Option<String> {
    let value: Value = serde_json::from_str(content).ok()?;
    for field in ["display_name", "name"] {
        if let Some(name) = value.get(field).and_then(Value::as_str) {
            if !name.trim().is_empty() {
                return Some(name.trim().to_owned());
            }
        }
    }
    None
}

fn abbreviated_pubkey(pubkey: &str) -> String {
    if pubkey.len() <= 16 {
        return pubkey.to_owned();
    }
    format!("{}…{}", &pubkey[..8], &pubkey[pubkey.len() - 6..])
}

pub(crate) fn escape_slack_label(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace(['\r', '\n', '*', '_', '`'], " ")
        .chars()
        .take(160)
        .collect()
}

/// Slack interprets angle-bracket control sequences as mentions and links.
/// Escape Buzz-authored content before posting it into an externally shared
/// channel so a string such as `<!channel>` cannot become a mass mention.
pub(crate) fn escape_slack_message_body(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::ToBech32;

    #[test]
    fn slack_origin_is_deterministic_and_threaded() {
        let keys = Keys::generate();
        let channel = Uuid::new_v4();
        let root = EventId::from_hex(&"a".repeat(64)).unwrap();
        let first = build_slack_origin_event(
            &keys,
            SlackOriginInput {
                buzz_channel_id: channel,
                content: "hello",
                team_id: "T12345678",
                channel_id: "C12345678",
                slack_ts: "1700000000.000001",
                user_id: "U12345678",
                thread_ts: Some("1699999999.000001"),
                reply_to: Some(root),
                media_tags: &[],
            },
        )
        .unwrap();
        let second = build_slack_origin_event(
            &keys,
            SlackOriginInput {
                buzz_channel_id: channel,
                content: "hello",
                team_id: "T12345678",
                channel_id: "C12345678",
                slack_ts: "1700000000.000001",
                user_id: "U12345678",
                thread_ts: Some("1699999999.000001"),
                reply_to: Some(root),
                media_tags: &[],
            },
        )
        .unwrap();
        assert_eq!(first.id, second.id);
        assert!(has_slack_origin(&first));
        assert_eq!(event_thread_root(&first), Some("a".repeat(64)));
        assert_eq!(event_channel_id(&first), Some(channel));
    }

    #[test]
    fn slack_origin_content_with_files_and_no_text() {
        let content = compose_slack_origin_content(
            "ram",
            "",
            "",
            &[
                "![image](https://b/media/aa.png)".into(),
                "📎 big.mov (too large to copy) — [open in Slack](https://s/f)".into(),
            ],
            &HashMap::new(),
        );
        assert_eq!(
            content,
            "**ram · Slack**\n![image](https://b/media/aa.png)\n📎 big.mov (too large to copy) — [open in Slack](https://s/f)"
        );
    }

    #[test]
    fn slack_origin_content_uses_mention_names() {
        let names = HashMap::from([("U1".to_owned(), "Jumair".to_owned())]);
        assert_eq!(
            compose_slack_origin_content("ram", "", "<@U1> see PR", &[], &names),
            "**ram · Slack**\n@Jumair see PR"
        );
    }

    #[test]
    fn replaces_npub_mentions_with_names() {
        let keys = Keys::generate();
        let hex = keys.public_key().to_hex();
        let npub = keys.public_key().to_bech32().unwrap();
        let other = Keys::generate().public_key().to_bech32().unwrap();
        let content = format!("hi nostr:{npub}, cc nostr:{other} and nostr:npub1short");
        assert_eq!(npub_mentions(&content).len(), 2);
        let names = HashMap::from([(hex, "Ram".to_owned())]);
        assert_eq!(
            replace_npub_mentions(&content, &names),
            format!("hi @Ram, cc nostr:{other} and nostr:npub1short")
        );
    }

    #[test]
    fn slack_origin_content_text_only_is_unchanged() {
        assert_eq!(
            compose_slack_origin_content("ram", "", "hello", &[], &HashMap::new()),
            "**ram · Slack**\nhello"
        );
    }

    #[test]
    fn slack_origin_event_carries_imeta_tags() {
        let keys = Keys::generate();
        let media = vec![vec![
            "imeta".to_string(),
            "url https://b/media/aa.png".into(),
            "m image/png".into(),
        ]];
        let event = build_slack_origin_event(
            &keys,
            SlackOriginInput {
                buzz_channel_id: Uuid::new_v4(),
                content: "x",
                team_id: "T1",
                channel_id: "C1",
                slack_ts: "1790000000.000100",
                user_id: "U1",
                thread_ts: None,
                reply_to: None,
                media_tags: &media,
            },
        )
        .unwrap();
        assert!(event
            .tags
            .iter()
            .any(|t| t.as_slice().first().map(String::as_str) == Some("imeta")));
    }

    #[test]
    fn buzz_comment_escapes_and_lists_failures() {
        let comment = compose_buzz_comment(
            "ram",
            "",
            "see <!channel>",
            &["📎 big.mp4 (too large to copy) — see Buzz".into()],
        );
        assert_eq!(
            comment,
            "*ram · Buzz*\nsee &lt;!channel&gt;\n📎 big.mp4 (too large to copy) — see Buzz"
        );
    }

    #[test]
    fn buzz_comment_without_text() {
        assert_eq!(compose_buzz_comment("ram", "", "", &[]), "*ram · Buzz*\n");
    }

    #[test]
    fn direct_and_nested_replies_resolve_to_root() {
        let keys = Keys::generate();
        let root = "a".repeat(64);
        let parent = "b".repeat(64);
        let direct = EventBuilder::new(Kind::Custom(9), "direct")
            .tags([Tag::parse(["e", root.as_str(), "", "reply"]).unwrap()])
            .sign_with_keys(&keys)
            .unwrap();
        assert_eq!(event_thread_root(&direct), Some(root.clone()));

        let nested = EventBuilder::new(Kind::Custom(9), "nested")
            .tags([
                Tag::parse(["e", root.as_str(), "", "root"]).unwrap(),
                Tag::parse(["e", parent.as_str(), "", "reply"]).unwrap(),
            ])
            .sign_with_keys(&keys)
            .unwrap();
        assert_eq!(event_thread_root(&nested), Some(root));
    }

    #[test]
    fn profile_name_prefers_display_name() {
        assert_eq!(
            profile_display_name(r#"{"name":"alice","display_name":"Alice A."}"#),
            Some("Alice A.".into())
        );
        assert_eq!(profile_display_name("{}"), None);
    }

    #[test]
    fn install_token_cannot_cross_team_boundaries() {
        let config = Config {
            relay_url: "ws://localhost:3000".into(),
            bridge_keys: Keys::generate(),
            owner_auth_tag: None,
            slack_signing_secret: "secret".into(),
            slack_bot_token: "token".into(),
            listen_addr: "127.0.0.1:3100".parse().unwrap(),
            state_path: "state.json".into(),
            allow_non_shared_channels: false,
            replay_lookback_secs: 60,
            display_name: "Slack Connect Bridge".to_owned(),
            max_file_bytes: 104_857_600,
            channels: vec![ChannelMapping {
                slack_team_id: "T12345678".into(),
                slack_channel_id: "C12345678".into(),
                buzz_channel_id: Uuid::new_v4(),
            }],
        };
        let error = validate_installation(&config, "T87654321")
            .unwrap_err()
            .to_string();
        assert!(error.contains("one bridge process per Slack installation"));
    }

    #[test]
    fn buzz_content_cannot_create_slack_control_mentions() {
        assert_eq!(
            escape_slack_message_body("deploy <!channel> and <@U12345678>"),
            "deploy &lt;!channel&gt; and &lt;@U12345678&gt;"
        );
    }
}
