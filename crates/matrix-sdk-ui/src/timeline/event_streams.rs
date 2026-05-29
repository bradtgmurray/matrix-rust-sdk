// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures_util::StreamExt as _;
use matrix_sdk::{
    Room,
    event_streams::{
        EventStreamSubscriberUpdate, EventStreamSubscription as SdkEventStreamSubscription,
        StreamId,
    },
    ruma::{MilliSecondsSinceUnixEpoch, OwnedEventId},
    task_monitor::BackgroundTaskHandle,
};
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, trace};

use super::{Timeline, controller::TimelineController};

/// A handle for subscribing to event streams displayed by a focused timeline.
#[derive(Debug)]
pub struct EventStreamSubscription {
    inner: Arc<EventStreamSubscriptionInner>,
    _timeline_update_task: BackgroundTaskHandle,
    _sdk_update_task: BackgroundTaskHandle,
}

#[derive(Debug)]
struct EventStreamSubscriptionInner {
    controller: TimelineController,
    room: Room,
    subscriptions: Mutex<HashMap<OwnedEventId, SdkEventStreamSubscription>>,
    cancelled: Mutex<HashSet<OwnedEventId>>,
}

impl EventStreamSubscription {
    pub(super) async fn new(timeline: &Timeline) -> Self {
        let room = timeline.room().clone();
        let inner = Arc::new(EventStreamSubscriptionInner {
            controller: timeline.controller.clone(),
            room: room.clone(),
            subscriptions: Default::default(),
            cancelled: Default::default(),
        });

        let (_, mut timeline_updates) = timeline.controller.subscribe().await;
        let mut sdk_updates = room.client().event_streams().subscriptions().subscribe_to_updates();

        trace!(room_id = %room.room_id(), "starting timeline event stream subscription monitor");
        inner.refresh().await;

        let timeline_update_inner = inner.clone();
        let timeline_update_task = room
            .client()
            .task_monitor()
            .spawn_infinite_task("timeline::event_stream_messages", async move {
                while timeline_updates.next().await.is_some() {
                    timeline_update_inner.refresh().await;
                }
            })
            .abort_on_drop();

        let sdk_update_inner = inner.clone();
        let sdk_update_task = room
            .client()
            .task_monitor()
            .spawn_infinite_task("timeline::event_stream_updates", async move {
                loop {
                    match sdk_updates.recv().await {
                        Ok(update) => sdk_update_inner.handle_update(update).await,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            })
            .abort_on_drop();

        Self {
            inner,
            _timeline_update_task: timeline_update_task,
            _sdk_update_task: sdk_update_task,
        }
    }

    /// Unsubscribe from all streams currently displayed by this timeline.
    pub async fn unsubscribe(self) {
        self._timeline_update_task.abort();
        self._sdk_update_task.abort();
        self.inner.unsubscribe_all().await;
    }
}

impl EventStreamSubscriptionInner {
    async fn refresh(&self) {
        let displayed_streams = self
            .controller
            .items()
            .await
            .iter()
            .filter_map(|item| {
                let event = item.as_event()?;
                let event_id = event.event_id()?.to_owned();
                let message = event.content().as_message()?;
                let descriptor = message.stream()?;
                if descriptor.expiry_ms.is_some_and(|expiry_ms| {
                    u64::from(event.timestamp().0)
                        .saturating_add(u64::try_from(expiry_ms.as_millis()).unwrap_or(u64::MAX))
                        <= u64::from(MilliSecondsSinceUnixEpoch::now().0)
                }) {
                    return None;
                }

                Some((event_id, message.body().to_owned()))
            })
            .collect::<HashMap<_, _>>();

        let removed = {
            let mut subscriptions = self.subscriptions.lock().await;
            let removed_ids = subscriptions
                .keys()
                .filter(|event_id| !displayed_streams.contains_key(*event_id))
                .cloned()
                .collect::<Vec<_>>();
            removed_ids
                .into_iter()
                .filter_map(|event_id| subscriptions.remove(&event_id).map(|sub| (event_id, sub)))
                .collect::<Vec<_>>()
        };

        for (event_id, subscription) in removed {
            trace!(
                room_id = %self.room.room_id(),
                %event_id,
                "unsubscribing from event stream no longer displayed in timeline"
            );
            subscription.unsubscribe().await;
            self.controller.set_event_stream_transient_body(&event_id, None).await;
        }

        self.cancelled.lock().await.retain(|event_id| displayed_streams.contains_key(event_id));

        for (event_id, body) in displayed_streams {
            if self.cancelled.lock().await.contains(&event_id) {
                continue;
            }

            if self.subscriptions.lock().await.contains_key(&event_id) {
                let stream_id = StreamId::new(self.room.room_id().to_owned(), event_id.clone());
                if let Some(body) = self
                    .room
                    .client()
                    .event_streams()
                    .subscriptions()
                    .transient_body(&stream_id)
                    .await
                {
                    self.controller.set_event_stream_transient_body(&event_id, Some(body)).await;
                }
                continue;
            }

            match self.room.subscribe_to_event_stream(&event_id).await {
                Ok(subscription) => {
                    trace!(
                        room_id = %self.room.room_id(),
                        %event_id,
                        initial_body_chars = body.chars().count(),
                        "subscribed to displayed event stream"
                    );
                    self.subscriptions.lock().await.insert(event_id.clone(), subscription);
                    self.controller.set_event_stream_transient_body(&event_id, Some(body)).await;
                }
                Err(error) => {
                    debug!(
                        room_id = %self.room.room_id(),
                        %event_id,
                        "failed to subscribe to displayed event stream: {error}"
                    );
                }
            }
        }
    }

    async fn handle_update(&self, update: EventStreamSubscriberUpdate) {
        let (stream_id, transient_body, cancelled, operation, appended_chars) = match update {
            EventStreamSubscriberUpdate::Replaced { stream_id, body } => {
                (stream_id, Some(body), false, "replace", None)
            }
            EventStreamSubscriberUpdate::Appended { stream_id, appended_body, body } => {
                (stream_id, Some(body), false, "append", Some(appended_body.chars().count()))
            }
            EventStreamSubscriberUpdate::Cancelled { stream_id, .. } => {
                (stream_id, None, true, "cancel", None)
            }
            EventStreamSubscriberUpdate::Expired { stream_id } => {
                (stream_id, None, true, "expire", None)
            }
        };

        if stream_id.room_id != self.room.room_id() {
            return;
        }
        if !self.subscriptions.lock().await.contains_key(&stream_id.event_id) {
            return;
        }

        if cancelled {
            self.subscriptions.lock().await.remove(&stream_id.event_id);
            self.cancelled.lock().await.insert(stream_id.event_id.clone());
        }
        trace!(
            room_id = %stream_id.room_id,
            event_id = %stream_id.event_id,
            operation,
            ?appended_chars,
            transient_body_chars = ?transient_body.as_deref().map(|body| body.chars().count()),
            "applying event stream update to timeline message"
        );
        self.controller.set_event_stream_transient_body(&stream_id.event_id, transient_body).await;
    }

    async fn unsubscribe_all(&self) {
        let subscriptions = std::mem::take(&mut *self.subscriptions.lock().await);
        for (event_id, subscription) in subscriptions {
            trace!(
                room_id = %self.room.room_id(),
                %event_id,
                "unsubscribing from timeline event stream"
            );
            subscription.unsubscribe().await;
            self.controller.set_event_stream_transient_body(&event_id, None).await;
        }
    }
}
