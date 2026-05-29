use std::{env, sync::Arc, time::Instant};

use anyhow::{Context as _, Result, bail};
use clap::Parser;
use futures_util::StreamExt as _;
use matrix_sdk::{
    Client, Room, RoomState,
    config::SyncSettings,
    encryption::{EncryptionSettings, recovery::RecoveryState},
    event_streams::{EventStreamPublisher, EventStreamPublisherOptions},
    ruma::{
        OwnedUserId,
        events::room::{
            member::StrippedRoomMemberEvent,
            message::{
                MessageType, OriginalSyncRoomMessageEvent, RoomMessageEventContent,
                RoomMessageEventContentWithoutRelation,
            },
        },
    },
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::json;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
struct Cli {
    /// The URL of the Matrix homeserver.
    homeserver_url: String,

    /// The Matrix account name used by the bot.
    username: String,

    /// The password for the bot account.
    password: String,

    /// Automatically join rooms that invite the bot.
    #[arg(long)]
    auto_join: bool,

    /// Initialize or unlock Matrix recovery/key backup with this passphrase.
    #[arg(long, env = "MATRIX_RECOVERY_PASSPHRASE", hide_env_values = true)]
    recovery_passphrase: Option<String>,
}

#[derive(Clone)]
struct OpenAi {
    api_key: Arc<str>,
    model: Arc<str>,
    http: reqwest::Client,
}

impl OpenAi {
    fn from_env() -> Result<Self> {
        let api_key = env::var("OPENAI_API_KEY").context("OPENAI_API_KEY must be set")?;
        let model = env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4.1-mini".to_owned());

        Ok(Self { api_key: api_key.into(), model: model.into(), http: reqwest::Client::new() })
    }

    async fn stream_text(&self, input: &str, publisher: &EventStreamPublisher) -> Result<String> {
        let started = Instant::now();
        info!(
            room_id = %publisher.stream_id().room_id,
            event_id = %publisher.stream_id().event_id,
            model = %self.model,
            prompt_chars = input.chars().count(),
            "starting Responses API streaming request"
        );

        let response = self
            .http
            .post("https://api.openai.com/v1/responses")
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
            .header(CONTENT_TYPE, "application/json")
            .json(&json!({
                "model": self.model.as_ref(),
                "input": input,
                "stream": true,
            }))
            .send()
            .await?;
        info!(
            status = %response.status(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "received Responses API response headers"
        );
        let response = response.error_for_status()?;

        let mut pending = Vec::new();
        let mut answer = String::new();
        let mut chunk_index = 0usize;
        let mut event_index = 0usize;
        let mut delta_index = 0usize;

        let mut bytes = response.bytes_stream();
        while let Some(chunk) = bytes.next().await {
            let chunk = chunk?;
            chunk_index += 1;
            let buffered_before = pending.len();
            pending.extend_from_slice(&chunk);
            let events = take_sse_data(&mut pending)?;

            info!(
                chunk_index,
                chunk_bytes = chunk.len(),
                buffered_before,
                buffered_after = pending.len(),
                available_sse_events = events.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "received Responses API byte chunk"
            );

            for data in events {
                event_index += 1;
                if data == "[DONE]" {
                    info!(event_index, "received Responses API done marker");
                    continue;
                }

                let event: serde_json::Value = serde_json::from_str(&data)?;
                let event_type = event["type"].as_str().unwrap_or("<missing>");
                info!(
                    event_index,
                    event_type,
                    payload_bytes = data.len(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "decoded Responses API SSE event"
                );

                match event_type {
                    "response.output_text.delta" => {
                        if let Some(delta) = event["delta"].as_str() {
                            delta_index += 1;
                            answer.push_str(delta);
                            info!(
                                delta_index,
                                delta_bytes = delta.len(),
                                delta_chars = delta.chars().count(),
                                accumulated_chars = answer.chars().count(),
                                delta = ?delta,
                                "queueing Matrix event stream append for Responses API text delta"
                            );
                            publisher.append(delta).await?;
                            info!(
                                delta_index,
                                room_id = %publisher.stream_id().room_id,
                                event_id = %publisher.stream_id().event_id,
                                "queued Matrix event stream append"
                            );
                        }
                    }
                    "response.failed" | "error" => {
                        let message = event
                            .pointer("/response/error/message")
                            .or_else(|| event.pointer("/error/message"))
                            .or_else(|| event.get("message"))
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("no error message returned");
                        bail!("Responses API stream failed: {message}");
                    }
                    _ => {}
                }
            }
        }

        if !pending.is_empty() {
            warn!(
                buffered_bytes = pending.len(),
                "Responses API byte stream ended with an incomplete SSE payload"
            );
        }
        info!(
            http_chunks = chunk_index,
            sse_events = event_index,
            text_deltas = delta_index,
            answer_bytes = answer.len(),
            answer_chars = answer.chars().count(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Responses API byte stream completed"
        );

        if answer.is_empty() {
            bail!("Responses API stream completed without text output");
        }

        Ok(answer)
    }
}

fn take_sse_data(pending: &mut Vec<u8>) -> Result<Vec<String>> {
    let mut payloads = Vec::new();

    while let Some((separator, separator_len)) = find_sse_separator(pending) {
        let event = pending.drain(..separator + separator_len).collect::<Vec<_>>();
        let event = std::str::from_utf8(&event)?;
        let data = event
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .collect::<Vec<_>>()
            .join("\n");

        if !data.is_empty() {
            payloads.push(data);
        }
    }

    Ok(payloads)
}

fn find_sse_separator(bytes: &[u8]) -> Option<(usize, usize)> {
    let lf = bytes.windows(2).position(|window| window == b"\n\n").map(|index| (index, 2));
    let crlf = bytes.windows(4).position(|window| window == b"\r\n\r\n").map(|index| (index, 4));

    match (lf, crlf) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(found), None) | (None, Some(found)) => Some(found),
        (None, None) => None,
    }
}

fn request_without_mention(body: &str, own_user_id: &OwnedUserId) -> Option<String> {
    // `m.mentions` identifies the user but carries no source-text range. This
    // example strips the MXID representation from the plaintext body.
    let prompt = body.replace(own_user_id.as_str(), "");
    let prompt = prompt.trim().trim_start_matches(&[':', ','][..]).trim();

    (!prompt.is_empty()).then(|| prompt.to_owned())
}

async fn on_room_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    own_user_id: OwnedUserId,
    openai: OpenAi,
) -> Result<()> {
    if room.state() != RoomState::Joined || event.sender == own_user_id {
        return Ok(());
    }

    let Some(mentions) = event.content.mentions.as_ref() else {
        return Ok(());
    };
    if !mentions.user_ids.contains(&own_user_id) {
        return Ok(());
    }

    let MessageType::Text(text) = &event.content.msgtype else {
        return Ok(());
    };
    let Some(prompt) = request_without_mention(&text.body, &own_user_id) else {
        return Ok(());
    };

    info!(
        room_id = %room.room_id(),
        source_event_id = %event.event_id,
        sender = %event.sender,
        prompt_chars = prompt.chars().count(),
        "received mention; creating Matrix event stream response"
    );
    let publisher = room
        .send_streaming_message(
            RoomMessageEventContent::text_plain(""),
            EventStreamPublisherOptions::default(),
        )
        .await?;
    info!(
        room_id = %publisher.stream_id().room_id,
        event_id = %publisher.stream_id().event_id,
        "sent Matrix event stream descriptor"
    );

    let final_body = match openai.stream_text(&prompt, &publisher).await {
        Ok(answer) => answer,
        Err(error) => {
            eprintln!("OpenAI request failed: {error:#}");
            format!("OpenAI request failed: {error}")
        }
    };

    let stream_id = publisher.stream_id().clone();
    info!(
        room_id = %stream_id.room_id,
        event_id = %stream_id.event_id,
        final_body_bytes = final_body.len(),
        final_body_chars = final_body.chars().count(),
        "finishing Matrix event stream with final replacement"
    );
    publisher.finish(RoomMessageEventContentWithoutRelation::text_plain(final_body)).await?;
    info!(
        room_id = %stream_id.room_id,
        event_id = %stream_id.event_id,
        "finished Matrix event stream"
    );
    Ok(())
}

async fn on_invite(room_member: StrippedRoomMemberEvent, client: Client, room: Room) {
    if Some(room_member.state_key.as_ref()) != client.user_id() {
        return;
    }

    tokio::spawn(async move {
        println!("Joining invited room {}", room.room_id());
        let mut delay = Duration::from_secs(2);

        loop {
            match room.join().await {
                Ok(()) => {
                    println!("Joined room {}", room.room_id());
                    return;
                }
                Err(error) if delay <= Duration::from_secs(3600) => {
                    eprintln!(
                        "Unable to join room {} ({error:?}), retrying in {}s",
                        room.room_id(),
                        delay.as_secs()
                    );
                    sleep(delay).await;
                    delay *= 2;
                }
                Err(error) => {
                    eprintln!("Unable to join room {}: {error:?}", room.room_id());
                    return;
                }
            }
        }
    });
}

async fn setup_recovery(client: &Client, passphrase: Option<&str>) -> Result<()> {
    let Some(passphrase) = passphrase else {
        return Ok(());
    };

    client.encryption().wait_for_e2ee_initialization_tasks().await;

    let recovery = client.encryption().recovery();
    match recovery.state() {
        RecoveryState::Disabled => {
            println!("Enabling Matrix recovery and room key backup");
            recovery
                .enable()
                .with_passphrase(passphrase)
                .await
                .context("unable to enable Matrix recovery and room key backup")?;
        }
        RecoveryState::Incomplete => {
            println!("Recovering Matrix secrets and room key backup");
            recovery
                .recover(passphrase)
                .await
                .context("unable to recover Matrix secrets and room key backup")?;
        }
        RecoveryState::Enabled => {
            println!("Matrix recovery and room key backup are already enabled");
        }
        RecoveryState::Unknown => {
            bail!("Matrix recovery state is still unknown after E2EE initialization");
        }
    }

    Ok(())
}

async fn login_and_sync(
    homeserver_url: String,
    username: String,
    password: String,
    openai: OpenAi,
    auto_join: bool,
    recovery_passphrase: Option<String>,
) -> Result<()> {
    let enable_recovery = recovery_passphrase.is_some();
    let client = Client::builder()
        .homeserver_url(homeserver_url)
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: enable_recovery,
            auto_enable_backups: enable_recovery,
            ..Default::default()
        })
        .build()
        .await?;
    client
        .matrix_auth()
        .login_username(&username, &password)
        .initial_device_display_name("event stream AI bot")
        .await?;

    let own_user_id = client.user_id().context("login did not return a user ID")?.to_owned();

    if auto_join {
        client.add_event_handler(on_invite);
    }

    setup_recovery(&client, recovery_passphrase.as_deref()).await?;

    let response = client.sync_once(SyncSettings::default()).await?;

    client.add_event_handler(move |event, room| {
        let own_user_id = own_user_id.clone();
        let openai = openai.clone();
        async move {
            // Do not block sync response handling while streaming; the sync loop needs
            // to receive subscribers' to-device requests and deliver updates.
            tokio::spawn(async move {
                if let Err(error) = on_room_message(event, room, own_user_id, openai).await {
                    eprintln!("unable to handle AI request: {error}");
                }
            });
        }
    });

    client.sync(SyncSettings::default().token(response.next_batch)).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,matrix_sdk::event_streams=trace")),
        )
        .init();

    let cli = Cli::parse();

    login_and_sync(
        cli.homeserver_url,
        cli.username,
        cli.password,
        OpenAi::from_env()?,
        cli.auto_join,
        cli.recovery_passphrase,
    )
    .await
}
