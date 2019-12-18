use std::time::{Duration, Instant};

use futures::compat::Future01CompatExt;
use rusoto_sqs::{DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, Sqs, SqsClient};
use rusoto_sqs::Message as SqsMessage;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::completion_event_serializer::CompletionEventSerializer;
use crate::event_emitter::EventEmitter;

pub struct CompletionPolicy {
    max_messages: u16,
    max_time_between_flushes: Duration,
    last_flush: Instant,
}

impl CompletionPolicy {
    pub fn new(max_messages: u16, max_time_between_flushes: Duration) -> Self {
        Self {
            max_messages, max_time_between_flushes, last_flush: Instant::now()
        }
    }

    pub fn should_flush(&self, cur_messages: u16) -> bool {
        cur_messages >= self.max_messages ||
            Instant::now().checked_duration_since(self.last_flush).unwrap() >= self.max_time_between_flushes
    }

    pub fn set_last_flush(&mut self) {
        self.last_flush = Instant::now();
    }
}

pub struct SqsCompletionHandler<CPE, CP, CE, Payload, EE>
where
    CPE: Send + Sync + 'static,
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload, Error=CPE> + Send + Sync  + 'static,
    Payload: Send + Sync + 'static,
    CE: Send + Sync  + 'static,
    EE: EventEmitter<Event = Payload> + Send + Sync  + 'static,
{
    sqs_client: SqsClient,
    queue_url: String,
    completed_events: Vec<CE>,
    completed_messages: Vec<SqsMessage>,
    completion_serializer: CP,
    event_emitter: EE,
    completion_policy: CompletionPolicy,
}

impl<CPE, CP, CE, Payload, EE> SqsCompletionHandler<CPE, CP, CE, Payload, EE>
where
    CPE: Send + Sync + 'static,
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload, Error=CPE> + Send + Sync  + 'static,
    Payload: Send + Sync + 'static,
    CE: Send + Sync  + 'static,
    EE: EventEmitter<Event = Payload> + Send + Sync  + 'static,
{
    pub fn new(
        sqs_client: SqsClient,
        queue_url: String,
        completion_serializer: CP,
        event_emitter: EE,
        completion_policy: CompletionPolicy,
    ) -> Self {
        Self {
            sqs_client,
            queue_url,
            completed_events: Vec::with_capacity(10),
            completed_messages: Vec::with_capacity(10),
            completion_serializer,
            event_emitter,
            completion_policy
        }
    }
}

impl<CPE, CP, CE, Payload, EE> SqsCompletionHandler<CPE, CP, CE, Payload, EE>
where
    CPE: Send + Sync + 'static,
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload, Error=CPE> + Send + Sync + 'static,
    Payload: Send + Sync + 'static,
    CE: Send + Sync  + 'static,
    EE: EventEmitter<Event = Payload> + Send + Sync  + 'static,
{
    pub async fn mark_complete(&mut self, sqs_message: SqsMessage, completed: CE) {
        self.completed_events.push(completed);
        self.completed_messages.push(sqs_message);

        if self.completion_policy.should_flush(self.completed_events.len() as u16) {
            let serialized_event = self
                .completion_serializer
                .serialize_completed_events(&self.completed_events[..]);

            let serialized_event = match serialized_event {
                Ok(serialized_event) => serialized_event,
                Err(e) => {
                    // We should emit a failure, but ultimately we just have to not ack these messages

                    self.completed_events.clear();
                    self.completed_messages.clear();

                    return
                }
            };
            
            // TODO: Retry on failure
            self.event_emitter.emit_event(serialized_event).await.unwrap();

            self.ack_all().await;
            self.completion_policy.set_last_flush();
        }
    }
    pub async fn ack_all(&mut self) {

        let acks =
        self.completed_messages.chunks(10).map(|chunk| {
            let entries = chunk.iter().map(|msg| {
                DeleteMessageBatchRequestEntry {
                    id: "".to_string(),
                    receipt_handle: msg.receipt_handle.clone().expect("Message missing receipt")
                }
            }).collect();

            self.sqs_client.delete_message_batch(
                DeleteMessageBatchRequest {
                    entries,
                    queue_url: self.queue_url.clone()
                }
            ).with_timeout(Duration::from_secs(2)).compat()
        });

        futures::future::join_all(acks).await;

        self.completed_events.clear();
        self.completed_messages.clear();
    }
}

#[allow(non_camel_case_types)]
pub enum SqsCompletionHandlerMessage<CE>
where
    CE: Send + Sync  + 'static,
{
    mark_complete { msg: SqsMessage, completed: CE },
    ack_all {},
}

impl<CPE, CP, CE, Payload, EE> SqsCompletionHandler<CPE, CP, CE, Payload, EE>
where
    CPE: Send + Sync + 'static,
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload, Error=CPE> + Send + Sync  + 'static,
    Payload: Send + Sync + 'static,
    CE: Send + Sync  + 'static,
    EE: EventEmitter<Event = Payload> + Send + Sync  + 'static,
{
    pub async fn route_message(&mut self, msg: SqsCompletionHandlerMessage<CE>) {
        match msg {
            SqsCompletionHandlerMessage::mark_complete { msg, completed } => {
                self.mark_complete(msg, completed).await
            }
            SqsCompletionHandlerMessage::ack_all {} => self.ack_all().await,
        };
    }
}

#[derive(Clone)]
pub struct SqsCompletionHandlerActor<CE>
where
    CE: Send + Sync  + 'static,
{
    sender: Sender<SqsCompletionHandlerMessage<CE>>,
}

impl<CE> SqsCompletionHandlerActor<CE>
where
    CE: Send + Sync  + 'static,
{
    pub fn new<CPE, CP, Payload, EE>(actor_impl: SqsCompletionHandler<CPE, CP, CE, Payload, EE>) -> Self
    where
        CPE: Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload, Error=CPE>
            + Send
            + Sync
            
            + 'static,
        Payload: Send + Sync + 'static,
        EE: EventEmitter<Event = Payload> + Send + Sync  + 'static,
    {
        let (sender, receiver) = channel(1);

        tokio::task::spawn(
            route_wrapper(
                SqsCompletionHandlerRouter {
                    receiver,
                    actor_impl,
                }
            )
        );

        Self { sender }
    }

    pub async fn mark_complete(&self, msg: SqsMessage, completed: CE) {
        let msg = SqsCompletionHandlerMessage::mark_complete { msg, completed };
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. mark_complete")
        }
    }
    pub async fn ack_all(&self) {
        let msg = SqsCompletionHandlerMessage::ack_all {};
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. ack_all")
        }
    }
}

use async_trait::async_trait;

#[async_trait]
pub trait CompletionHandler {
    type Message;
    type CompletedEvent;

    async fn mark_complete(&self, msg: Self::Message, completed_event: Self::CompletedEvent);
}

#[async_trait]
impl<CE> CompletionHandler for SqsCompletionHandlerActor<CE>
where
    CE: Send + Sync  + 'static,
{
    type Message = SqsMessage;
    type CompletedEvent = CE;

    async fn mark_complete(&self, msg: Self::Message, completed_event: Self::CompletedEvent) {
        SqsCompletionHandlerActor::mark_complete(
            self, msg, completed_event
        ).await
    }
}

pub struct SqsCompletionHandlerRouter<CPE, CP, CE, Payload, EE>
where
    CPE: Send + Sync + 'static,
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload, Error=CPE> + Send + Sync  + 'static,
    Payload: Send + Sync + 'static,
    CE: Send + Sync  + 'static,
    EE: EventEmitter<Event = Payload> + Send + Sync  + 'static,
{
    receiver: Receiver<SqsCompletionHandlerMessage<CE>>,
    actor_impl: SqsCompletionHandler<CPE, CP, CE, Payload, EE>,
}


async fn route_wrapper<CPE, CP, CE, Payload, EE>(mut router: SqsCompletionHandlerRouter<CPE, CP, CE, Payload, EE>)
    where
        CPE: Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload, Error=CPE> + Send + Sync  + 'static,
        Payload: Send + Sync + 'static,
        CE: Send + Sync  + 'static,
        EE: EventEmitter<Event = Payload> + Send + Sync  + 'static,
{
    while let Some(msg) = router.receiver.recv().await {
        router.actor_impl.route_message(msg).await;
    }
}
