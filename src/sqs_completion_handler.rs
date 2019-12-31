use std::fmt::Debug;
use std::time::{Duration, Instant};

use futures::compat::Future01CompatExt;
use log::*;
use rusoto_sqs::{DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, Sqs, SqsClient};
use rusoto_sqs::Message as SqsMessage;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;

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
            max_messages,
            max_time_between_flushes,
            last_flush: Instant::now(),
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

pub struct SqsCompletionHandler<CPE, CP, CE, Payload, EE, OA>
    where
        CPE: Debug + Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent=CE, Output=Payload, Error=CPE> + Send + Sync + 'static,
        Payload: Send + Sync + 'static,
        CE: Send + Sync + Clone + 'static,
        EE: EventEmitter<Event=Payload> + Send + Sync + 'static,
        OA: Fn(SqsCompletionHandlerActor<CE>, Result<String, String>) + Send + Sync + 'static,
{
    sqs_client: SqsClient,
    queue_url: String,
    completed_events: Vec<CE>,
    completed_messages: Vec<SqsMessage>,
    completion_serializer: CP,
    event_emitter: EE,
    completion_policy: CompletionPolicy,
    on_ack: OA,
    self_actor: Option<SqsCompletionHandlerActor<CE>>
}

impl<CPE, CP, CE, Payload, EE, OA> SqsCompletionHandler<CPE, CP, CE, Payload, EE, OA>
    where
        CPE: Debug + Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent=CE, Output=Payload, Error=CPE> + Send + Sync + 'static,
        Payload: Send + Sync + 'static,
        CE: Send + Sync + Clone + 'static,
        EE: EventEmitter<Event=Payload> + Send + Sync + 'static,
        OA: Fn(SqsCompletionHandlerActor<CE>, Result<String, String>) + Send + Sync + 'static,
{
    pub fn new(
        sqs_client: SqsClient,
        queue_url: String,
        completion_serializer: CP,
        event_emitter: EE,
        completion_policy: CompletionPolicy,
        on_ack: OA,
    ) -> Self {
        Self {
            sqs_client,
            queue_url,
            completed_events: Vec::with_capacity(completion_policy.max_messages as usize),
            completed_messages: Vec::with_capacity(completion_policy.max_messages as usize),
            completion_serializer,
            event_emitter,
            completion_policy,
            on_ack,
            self_actor: None,
        }
    }
}

impl<CPE, CP, CE, Payload, EE, OA> SqsCompletionHandler<CPE, CP, CE, Payload, EE, OA>
    where
        CPE: Debug + Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent=CE, Output=Payload, Error=CPE> + Send + Sync + 'static,
        Payload: Send + Sync + 'static,
        CE: Send + Sync + Clone + 'static,
        EE: EventEmitter<Event=Payload> + Send + Sync + 'static,
        OA: Fn(SqsCompletionHandlerActor<CE>, Result<String, String>) + Send + Sync + 'static,
{
    pub async fn mark_complete(&mut self, sqs_message: SqsMessage, completed: CE, success: bool) {
        self.completed_events.push(completed);
        if success {
            self.completed_messages.push(sqs_message);
        }

        info!(
            "Marked event complete. {} completed events, {} completed messages",
            self.completed_events.len(),
            self.completed_messages.len(),
        );

        if self.completion_policy.should_flush(self.completed_events.len() as u16) {
            self.ack_all(None).await;
            self.completion_policy.set_last_flush();
        }
    }

    pub async fn ack_all(&mut self, notify: Option<tokio::sync::oneshot::Sender<()>>) {
        info!("Flushing completed events");
        let serialized_event = self
            .completion_serializer
            .serialize_completed_events(&self.completed_events[..]);

        let serialized_event = match serialized_event {
            Ok(serialized_event) => serialized_event,
            Err(e) => {
                // We should emit a failure, but ultimately we just have to not ack these messages
                self.completed_events.clear();
                self.completed_messages.clear();

                panic!("Serializing events failed: {:?}", e);
            }
        };

        info!("Emitting events");
        self.event_emitter.emit_event(serialized_event).await.expect(
            "Failed to emit event"
        );

        let mut acks = vec![];

        for chunk in self.completed_messages.chunks(10) {
            let msg_ids: Vec<String> = chunk.iter().map(|msg| {
                msg.message_id.clone().unwrap()
            }).collect();

            let entries = chunk.iter().map(|msg| {
                DeleteMessageBatchRequestEntry {
                    id: msg.message_id.clone().unwrap(),
                    receipt_handle: msg.receipt_handle.clone().expect("Message missing receipt"),
                }
            }).collect();

            let ack = (
                self.sqs_client.delete_message_batch(
                    DeleteMessageBatchRequest {
                        entries,
                        queue_url: self.queue_url.clone(),
                    }
                ).with_timeout(Duration::from_secs(2)).compat().await,
                msg_ids
            );

            acks.push(ack);
        };

        info!("Acking all messages");

        for (result, msg_ids) in acks {
            match result {
                Ok(batch_result) => {
                    for success in batch_result.successful {
                        (self.on_ack)(self.self_actor.clone().unwrap(), Ok(success.id))
                    }

                    for failure in batch_result.failed {
                        (self.on_ack)(self.self_actor.clone().unwrap(), Err(failure.id))
                    }
                }
                Err(e) => {
                    for msg_id in msg_ids {
                        (self.on_ack)(self.self_actor.clone().unwrap(), Err(msg_id))
                    }
                    warn!("Failed to acknowledge event: {:?}", e);
                }
            }
//            (self.on_ack)(result, message_id);
        }
        info!("Acked");

        self.completed_events.clear();
        self.completed_messages.clear();

        if let Some(notify) = notify {
            let _ = notify.send(());
        }
    }
}

#[allow(non_camel_case_types)]
pub enum SqsCompletionHandlerMessage<CE>
    where
        CE: Send + Sync + Clone + 'static,
{
    mark_complete { msg: SqsMessage, completed: CE , success: bool },
    ack_all { notify: Option<tokio::sync::oneshot::Sender<()>> },
}

impl<CPE, CP, CE, Payload, EE, OA> SqsCompletionHandler<CPE, CP, CE, Payload, EE, OA>
    where
        CPE: Debug + Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent=CE, Output=Payload, Error=CPE> + Send + Sync + 'static,
        Payload: Send + Sync + 'static,
        CE: Send + Sync + Clone + 'static,
        EE: EventEmitter<Event=Payload> + Send + Sync + 'static,
        OA: Fn(SqsCompletionHandlerActor<CE>, Result<String, String>) + Send + Sync + 'static,
{
    pub async fn route_message(&mut self, msg: SqsCompletionHandlerMessage<CE>) {
        match msg {
            SqsCompletionHandlerMessage::mark_complete { msg, completed, success } => {
                self.mark_complete(msg, completed, success).await
            }
            SqsCompletionHandlerMessage::ack_all { notify } => self.ack_all(notify).await,
        };
    }
}

#[derive(Clone)]
pub struct SqsCompletionHandlerActor<CE>
    where
        CE: Send + Sync + Clone + 'static,
{
    sender: Sender<SqsCompletionHandlerMessage<CE>>,
}

impl<CE> SqsCompletionHandlerActor<CE>
    where
        CE: Send + Sync + Clone + 'static,
{
    pub fn new<CPE, CP, Payload, EE, OA>(mut actor_impl: SqsCompletionHandler<CPE, CP, CE, Payload, EE, OA>) -> Self
        where
            CPE: Debug + Send + Sync + 'static,
            CP: CompletionEventSerializer<CompletedEvent=CE, Output=Payload, Error=CPE>
            + Send
            + Sync

            + 'static,
            Payload: Send + Sync + 'static,
            EE: EventEmitter<Event=Payload> + Send + Sync + 'static,
            OA: Fn(SqsCompletionHandlerActor<CE>, Result<String, String>) + Send + Sync + 'static,
    {
        let (sender, receiver) = channel(1);

        let self_actor = Self { sender };
        actor_impl.self_actor = Some(self_actor.clone());
        tokio::task::spawn(
            route_wrapper(
                SqsCompletionHandlerRouter {
                    receiver,
                    actor_impl,
                }
            )
        );

        self_actor
    }

    pub async fn mark_complete(&self, msg: SqsMessage, completed: CE, success: bool) {
        let msg = SqsCompletionHandlerMessage::mark_complete { msg, completed, success };
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. mark_complete")
        }
    }
    async fn ack_all(&self, notify: Option<tokio::sync::oneshot::Sender<()>>) {
        let msg = SqsCompletionHandlerMessage::ack_all { notify };
        if let Err(_e) = self.sender.clone().send(msg).await {
            panic!("Receiver has failed, propagating error. ack_all")
        }
    }
}

#[async_trait]
pub trait CompletionHandler {
    type Message;
    type CompletedEvent;

    async fn mark_complete(&self, msg: Self::Message, completed_event: Self::CompletedEvent, success: bool);
    async fn ack_all(&self, notify: Option<tokio::sync::oneshot::Sender<()>>);
}

#[async_trait]
impl<CE> CompletionHandler for SqsCompletionHandlerActor<CE>
    where
        CE: Send + Sync + Clone + 'static,
{
    type Message = SqsMessage;
    type CompletedEvent = CE;

    async fn mark_complete(&self, msg: Self::Message, completed_event: Self::CompletedEvent, success: bool) {
        SqsCompletionHandlerActor::mark_complete(
            self, msg, completed_event, success
        ).await
    }

    async fn ack_all(&self, notify: Option<tokio::sync::oneshot::Sender<()>>) {
        SqsCompletionHandlerActor::ack_all(
            self, notify,
        ).await
    }
}

pub struct SqsCompletionHandlerRouter<CPE, CP, CE, Payload, EE, OA>
    where
        CPE: Debug + Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent=CE, Output=Payload, Error=CPE> + Send + Sync + 'static,
        Payload: Send + Sync + 'static,
        CE: Send + Sync + Clone + 'static,
        EE: EventEmitter<Event=Payload> + Send + Sync + 'static,
        OA: Fn(SqsCompletionHandlerActor<CE>, Result<String, String>) + Send + Sync + 'static,
{
    receiver: Receiver<SqsCompletionHandlerMessage<CE>>,
    actor_impl: SqsCompletionHandler<CPE, CP, CE, Payload, EE, OA>,
}


async fn route_wrapper<CPE, CP, CE, Payload, EE, OA>(mut router: SqsCompletionHandlerRouter<CPE, CP, CE, Payload, EE, OA>)
    where
        CPE: Debug + Send + Sync + 'static,
        CP: CompletionEventSerializer<CompletedEvent=CE, Output=Payload, Error=CPE> + Send + Sync + 'static,
        Payload: Send + Sync + 'static,
        CE: Send + Sync + Clone + 'static,
        EE: EventEmitter<Event=Payload> + Send + Sync + 'static,
        OA: Fn(SqsCompletionHandlerActor<CE>, Result<String, String>) + Send + Sync + 'static,
{
    while let Some(msg) = router.receiver.recv().await {
        router.actor_impl.route_message(msg).await;
    }
}
