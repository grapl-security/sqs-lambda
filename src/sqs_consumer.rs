use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures::compat::Future01CompatExt;

use log::info;
use rusoto_sqs::{ReceiveMessageRequest, Sqs};
use rusoto_sqs::Message as SqsMessage;
use tokio::sync::mpsc::{channel, Sender};
use async_trait::async_trait;

use crate::event_processor::EventProcessorActor;
use lambda_runtime::Context;

pub struct ConsumePolicy {
    context: Context,
    stop_at: Duration
}

impl ConsumePolicy {
    pub fn new(context: Context, stop_at: Duration) -> Self {
        Self {
            context, stop_at
        }
    }

    pub fn should_consume(&self) -> bool {
        self.stop_at.as_millis() >= self.context.get_time_remaining_millis() as u128
    }
}

pub struct SqsConsumer<S>
    where S: Sqs  + Send + Sync + 'static
{
    sqs_client: S,
    queue_url: String,
    stored_events: Vec<SqsMessage>,
    consume_policy: ConsumePolicy,
    shutdown_subscriber: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<S> SqsConsumer<S>
    where S: Sqs  + Send + Sync + 'static
{
    pub fn new(
        sqs_client: S,
        queue_url: String,
        consume_policy: ConsumePolicy,
        shutdown_subscriber: tokio::sync::oneshot::Sender<()>,
    ) -> SqsConsumer<S>
    where
        S: Sqs,
    {
        Self {
            sqs_client,
            queue_url,
            stored_events: Vec::with_capacity(20),
            consume_policy,
            shutdown_subscriber: Some(shutdown_subscriber),
        }
    }
}

impl<S> SqsConsumer<S>
    where S: Sqs  + Send + Sync + 'static
{
    pub async fn get_new_event(&mut self, event_processor: EventProcessorActor) {
        let should_consume = self.consume_policy.should_consume();

        if self.stored_events.len() == 0 && should_consume {
            let new_events = self.batch_get_events().await.unwrap();
            self.stored_events.extend(new_events);
        }

        if self.stored_events.is_empty() && !should_consume {
            info!("No more events to process, and no time to consume more");
            let shutdown_subscriber = std::mem::replace(&mut self.shutdown_subscriber, None);
            match shutdown_subscriber {
                Some(shutdown_subscriber) => shutdown_subscriber.send(()).unwrap(),
                None => panic!("Attempted to shut down with empty shutdown_subscriber")
            }
        }

        if let Some(next_event) = self.stored_events.pop() {
            event_processor.process_event(next_event).await;
        }
    }

    pub async fn batch_get_events(&self) -> Result<Vec<SqsMessage>, Box<dyn Error>> {
        let recv = self.sqs_client.receive_message(
            ReceiveMessageRequest {
                max_number_of_messages: Some(10),
                queue_url: self.queue_url.clone(),
                wait_time_seconds: Some(1),
                ..Default::default()
            }
        ).compat().await?;



        Ok(recv.messages.unwrap_or(vec![]))
    }
}

#[allow(non_camel_case_types)]
pub enum SqsConsumerMessage {
    get_new_event {
        event_processor: EventProcessorActor,
    },
}

impl<S> SqsConsumer<S>
    where S: Sqs  + Send + Sync + 'static
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
    pub fn new<S>(actor_impl: SqsConsumer<S>) -> Self
        where S: Sqs  + Send + Sync + 'static
    {
        let (sender, receiver) = channel(1);

        tokio::task::spawn(
            route_wrapper(
                SqsConsumerRouter {
                    receiver,
                    actor_impl,
                }
            )
        );
        Self { sender }
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

pub struct SqsConsumerRouter<S>
    where S: Sqs  + Send + Sync + 'static
{
    receiver: tokio::sync::mpsc::Receiver<SqsConsumerMessage>,
    actor_impl: SqsConsumer<S>,
}



async fn route_wrapper<S>(mut router: SqsConsumerRouter<S>)
    where S: Sqs  + Send + Sync + 'static
{
    while let Some(msg) = router.receiver.recv().await {
        router.actor_impl.route_message(msg).await;
    }
}
