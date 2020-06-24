use std::time::Duration;

use futures::compat::Future01CompatExt;
use lambda_runtime::Context;
use log::{debug, error, warn};
use rusoto_sqs::Message as SqsMessage;
use rusoto_sqs::{ReceiveMessageError, ReceiveMessageRequest, Sqs};
use tokio::sync::mpsc::{channel, Sender};

use tracing::instrument;

use crate::consumer::Consumer;
use async_trait::async_trait;

use crate::completion_handler::CompletionHandler;
use crate::event_processor::EventProcessorActor;
use aktors::actor::Actor;
use std::marker::PhantomData;
use chrono::Utc;

pub struct ConsumePolicy {
    deadline: i64,
    stop_at: Duration,
    max_empty_receives: u16,
    empty_receives: u16,
}

pub trait IntoDeadline {
    fn into_deadline(self) -> i64;
}

impl IntoDeadline for Context {
    fn into_deadline(self) -> i64 { self.deadline }
}

impl IntoDeadline for i64 {
    fn into_deadline(self) -> i64 { self }
}

impl ConsumePolicy {
    pub fn new(deadline: impl IntoDeadline, stop_at: Duration, max_empty_receives: u16) -> Self {
        Self {
            deadline: deadline.into_deadline(),
            stop_at,
            max_empty_receives,
            empty_receives: 0,
        }
    }

    pub fn get_time_remaining_millis(&self) -> i64 {
        self.deadline - Utc::now().timestamp_millis()
    }

    pub fn should_consume(&self) -> bool {
        (self.stop_at.as_millis() <= self.get_time_remaining_millis() as u128)
            && self.empty_receives <= self.max_empty_receives
    }

    pub fn register_received(&mut self, any: bool) {
        if any {
            self.empty_receives = 0;
        } else {
            self.empty_receives += 1;
        }
    }
}

pub struct SqsConsumer<S, CH>
where
    S: Sqs + Send + Sync + 'static,
    CH: CompletionHandler + Clone + Send + Sync + 'static,
{
    sqs_client: S,
    queue_url: String,
    stored_events: Vec<SqsMessage>,
    consume_policy: ConsumePolicy,
    completion_handler: CH,
    shutdown_subscriber: Option<tokio::sync::oneshot::Sender<()>>,
    self_actor: Option<SqsConsumerActor<S, CH>>,
}

impl<S, CH> SqsConsumer<S, CH>
where
    S: Sqs + Send + Sync + 'static,
    CH: CompletionHandler + Clone + Send + Sync + 'static,
{
    pub fn new(
        sqs_client: S,
        queue_url: String,
        consume_policy: ConsumePolicy,
        completion_handler: CH,
        shutdown_subscriber: tokio::sync::oneshot::Sender<()>,
    ) -> SqsConsumer<S, CH>
    where
        S: Sqs,
    {
        Self {
            sqs_client,
            queue_url,
            stored_events: Vec::with_capacity(20),
            consume_policy,
            completion_handler,
            shutdown_subscriber: Some(shutdown_subscriber),
            self_actor: None,
        }
    }
}
impl<S: Sqs + Send + Sync + 'static, CH: CompletionHandler + Clone + Send + Sync + 'static>
    SqsConsumer<S, CH>
{
    #[instrument(skip(self))]
    pub async fn batch_get_events(
        &self,
        wait_time_seconds: i64,
    ) -> Result<Vec<SqsMessage>, rusoto_core::RusotoError<ReceiveMessageError>> {
        debug!("Calling receive_message");
        let recv = self.sqs_client.receive_message(ReceiveMessageRequest {
            max_number_of_messages: Some(10),
            queue_url: self.queue_url.clone(),
            wait_time_seconds: Some(wait_time_seconds),
            ..Default::default()
        });

        let recv = tokio::time::timeout(Duration::from_secs(wait_time_seconds as u64 + 2), recv)
            .await
            .expect("batch_get_events timed out")?;
        debug!("Called receive_message : {:?}", recv);

        Ok(recv.messages.unwrap_or(vec![]))
    }
}

#[derive_aktor::derive_actor]
impl<S: Sqs + Send + Sync + 'static, CH: CompletionHandler + Clone + Send + Sync + 'static>
    SqsConsumer<S, CH>
{
    #[instrument(skip(self, event_processor))]
    pub async fn get_new_event(&mut self, event_processor: EventProcessorActor<SqsMessage>) {
        debug!("New event request");
        let should_consume = self.consume_policy.should_consume();

        if self.stored_events.is_empty() && should_consume {
            let new_events = match self.batch_get_events(1).await {
                Ok(new_events) => new_events,
                Err(e) => {
                    error!("Failed to get new events with: {:?}", e);
                    tokio::time::delay_for(Duration::from_secs(1)).await;
                    self.self_actor
                        .clone()
                        .unwrap()
                        .get_next_event(event_processor)
                        .await;

                    // Register the empty receive on error
                    self.consume_policy
                        .register_received(false);
                    return;
                }
            };

            self.consume_policy
                .register_received(!new_events.is_empty());
            self.stored_events.extend(new_events);
        }

        if !should_consume {
            debug!("Done consuming, forcing ack");
            let (tx, shutdown_notify) = tokio::sync::oneshot::channel();

            // If we're past the point of consuming it's time to start acking
            self.completion_handler.ack_all(Some(tx)).await;

            let _ = shutdown_notify.await;
            debug!("Ack complete");
        }

        if self.stored_events.is_empty() && !should_consume {
            debug!("No more events to process, and we should not consume more");
            let shutdown_subscriber = std::mem::replace(&mut self.shutdown_subscriber, None);
            match shutdown_subscriber {
                Some(shutdown_subscriber) => {
                    shutdown_subscriber.send(()).unwrap();
                }
                None => debug!("Attempted to shut down with empty shutdown_subscriber"),
            };

            event_processor.stop_processing().await;
            drop(event_processor);
            return;
        }

        if let Some(next_event) = self.stored_events.pop() {
            debug!("Sending next event to processor");
            event_processor.process_event(next_event).await;
            debug!("Sent next event to processor");
        } else {
            tokio::time::delay_for(Duration::from_millis(500)).await;
            debug!("No events to send to processor");
            self.self_actor
                .clone()
                .unwrap()
                .get_next_event(event_processor)
                .await;
        }
    }

    pub async fn _p(&self, __p: PhantomData<(S, CH)>) {}
}

#[async_trait]
impl<S, CH> Consumer<SqsMessage> for SqsConsumerActor<S, CH>
where
    S: Sqs + Send + Sync + 'static,
    CH: CompletionHandler + Clone + Send + Sync + 'static,
{
    #[instrument(skip(self, event_processor))]
    async fn get_next_event(&self, event_processor: EventProcessorActor<SqsMessage>) {
        let msg = SqsConsumerMessage::get_new_event { event_processor };
        self.queue_len
            .clone()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut sender = self.sender.clone();
        tokio::task::spawn(async move {
            if let Err(e) = sender.send(msg).await {
                panic!(
                    concat!(
                        "Receiver has failed with {}, propagating error. ",
                        "SqsConsumerActor.get_next_event"
                    ),
                    e
                )
            }
        });
    }
}
