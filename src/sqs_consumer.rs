use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures::compat::Future01CompatExt;

use log::{info, warn};
use rusoto_sqs::{ReceiveMessageRequest, Sqs, ReceiveMessageResult, ReceiveMessageError};
use rusoto_sqs::Message as SqsMessage;
use tokio::sync::mpsc::{channel, Sender};
use async_trait::async_trait;

use crate::event_processor::EventProcessorActor;
use lambda_runtime::Context;
use crate::sqs_completion_handler::CompletionHandler;

pub struct ConsumePolicy {
    context: Context,
    stop_at: Duration,
    max_empty_receives: u8,
    empty_receives: u8
}

impl ConsumePolicy {
    pub fn new(context: Context, stop_at: Duration, max_empty_receives: u8) -> Self {
        Self {
            context, stop_at, max_empty_receives,
            empty_receives: 0
        }
    }

    pub fn should_consume(&self) -> bool {
        (self.stop_at.as_millis() <= self.context.get_time_remaining_millis() as u128)
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
        S: Sqs  + Send + Sync + 'static,
        CH: CompletionHandler + Send + Sync + 'static
{
    sqs_client: S,
    queue_url: String,
    stored_events: Vec<SqsMessage>,
    consume_policy: ConsumePolicy,
    completion_handler: CH,
    shutdown_subscriber: Option<tokio::sync::oneshot::Sender<()>>,
    self_actor: Option<SqsConsumerActor>
}

impl<S, CH> SqsConsumer<S, CH>
    where S: Sqs  + Send + Sync + 'static,
          CH: CompletionHandler + Send + Sync + 'static
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

impl<S, CH> SqsConsumer<S, CH>
    where S: Sqs  + Send + Sync + 'static,
          CH: CompletionHandler + Send + Sync + 'static
{
    pub async fn get_new_event(&mut self, event_processor: EventProcessorActor) {
        info!("New event request");
        let should_consume = self.consume_policy.should_consume();

        if self.stored_events.is_empty() && should_consume {
            let new_events = match self.batch_get_events(1).await {
                Ok(new_events) => new_events,
                Err(e) => {
                    warn!("Failed to get new events with: {:?}", e);
                    self.self_actor.clone().unwrap().get_next_event(event_processor).await;
                    return
                }
            };

            self.consume_policy.register_received(!new_events.is_empty());
            self.stored_events.extend(new_events);
        }

        if !should_consume {
            info!("Done consuming, forcing ack");
            let (tx, shutdown_notify) = tokio::sync::oneshot::channel();

            // If we're past the point of consuming it's time to start acking
            self.completion_handler.ack_all(Some(tx)).await;

            let _ = shutdown_notify.await;
            info!("Ack complete");
        }

        if self.stored_events.is_empty() && !should_consume {
            info!("No more events to process, and we should not consume more");
            let shutdown_subscriber = std::mem::replace(&mut self.shutdown_subscriber, None);
            match shutdown_subscriber {
                Some(shutdown_subscriber) => {
                    shutdown_subscriber.send(()).unwrap();
                },
                None => warn!("Attempted to shut down with empty shutdown_subscriber")
            };
        }

        if let Some(next_event) = self.stored_events.pop() {
            info!("Sending next event to processor");
            event_processor.process_event(next_event).await;
            info!("Sent next event to processor");
        } else {
            info!("No events to send to processor");
        }
    }

    pub async fn batch_get_events(&self, wait_time_seconds: i64) -> Result<Vec<SqsMessage>, rusoto_core::RusotoError<ReceiveMessageError>> {
        info!("Calling receive_message");
        let recv = self.sqs_client.receive_message(
            ReceiveMessageRequest {
                max_number_of_messages: Some(10),
                queue_url: self.queue_url.clone(),
                wait_time_seconds: Some(wait_time_seconds),
                ..Default::default()
            }
        ).with_timeout(Duration::from_secs(wait_time_seconds as u64 + 2)).compat().await?;

        info!("Called receive_message : {:?}", recv);


        Ok(recv.messages.unwrap_or(vec![]))
    }
}

#[allow(non_camel_case_types)]
pub enum SqsConsumerMessage {
    get_new_event {
        event_processor: EventProcessorActor,
    },
}

impl<S, CH> SqsConsumer<S, CH>
    where S: Sqs  + Send + Sync + 'static,
          CH: CompletionHandler + Send + Sync + 'static
{
    async fn route_message(&mut self, msg: SqsConsumerMessage) {
        match msg {
            SqsConsumerMessage::get_new_event { event_processor } => {
                self.get_new_event(event_processor).await
            }
        };
    }
}

#[derive(Clone)]
pub struct SqsConsumerActor {
    sender: Sender<SqsConsumerMessage>,
}

impl SqsConsumerActor {
    pub fn new<S, CH>(mut actor_impl: SqsConsumer<S, CH>) -> Self
        where S: Sqs  + Send + Sync + 'static,
              CH: CompletionHandler + Send + Sync + 'static
    {
        let (sender, receiver) = channel(1);

        let self_actor = Self {sender};

        actor_impl.self_actor = Some(self_actor.clone());

        tokio::task::spawn(
            route_wrapper(
                SqsConsumerRouter {
                    receiver,
                    actor_impl,
                }
            )
        );

        self_actor
    }

    pub async fn get_next_event(&self, event_processor: EventProcessorActor) {
        let msg = SqsConsumerMessage::get_new_event { event_processor };
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. get_new_event")
        }
    }
}

#[async_trait]
pub trait Consumer {
    async fn get_next_event(&self, event_processor: EventProcessorActor);
}

#[async_trait]
impl Consumer for SqsConsumerActor {
    async fn get_next_event(&self, event_processor: EventProcessorActor) {
        SqsConsumerActor::get_next_event(
            self, event_processor
        ).await
    }
}

pub struct SqsConsumerRouter<S, CH>
    where S: Sqs  + Send + Sync + 'static,
          CH: CompletionHandler + Send + Sync + 'static
{
    receiver: tokio::sync::mpsc::Receiver<SqsConsumerMessage>,
    actor_impl: SqsConsumer<S, CH>,
}



async fn route_wrapper<S, CH>(mut router: SqsConsumerRouter<S, CH>)
    where S: Sqs  + Send + Sync + 'static,
          CH: CompletionHandler + Send + Sync + 'static
{
    while let Some(msg) = router.receiver.recv().await {
        router.actor_impl.route_message(msg).await;
    }
}
