use std::fmt::Debug;

use derive_aktor::derive_actor;
use log::{info, warn};
use notify::event::Event as FsEvent;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;

use crate::completion_event_serializer::CompletionEventSerializer;
use crate::completion_handler::CompletionHandler;
use crate::error::Error;
use crate::event_emitter::EventEmitter;
use crate::event_handler::{Completion, OutputEvent};
use crate::fs_event_emitter::FsEventEmitter;

pub struct FsCompletionHandler<Err, CE, EventSerializer>
where
    Err: Debug + Send + Sync + 'static,
    CE: Send + Sync + Clone + 'static,
    EventSerializer: CompletionEventSerializer<
            CompletedEvent = CE,
            Output = Vec<u8>,
            Error = crate::error::Error<Err>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    self_actor: Option<FsCompletionHandlerActor<Err, CE, EventSerializer>>,
    completion_serializer: EventSerializer,
    completed_events: Vec<CE>,
    identities: Vec<Vec<u8>>,
    completed_messages: Vec<FsEvent>,
    event_emitter: FsEventEmitter,
}

impl<Err, CE, EventSerializer> FsCompletionHandler<Err, CE, EventSerializer>
where
    Err: Debug + Send + Sync + 'static,
    CE: Send + Sync + Clone + 'static,
    EventSerializer: CompletionEventSerializer<
            CompletedEvent = CE,
            Output = Vec<u8>,
            Error = crate::error::Error<Err>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    pub fn new(completion_serializer: EventSerializer, fs_event_emitter: FsEventEmitter) -> Self {
        Self {
            self_actor: None,
            completion_serializer,
            completed_events: vec![],
            identities: vec![],
            completed_messages: vec![],
            event_emitter: fs_event_emitter,
        }
    }
}

#[derive_actor]
impl<
        Err: Debug + Send + Sync + 'static,
        CE: Send + Sync + Clone + 'static,
        EventSerializer: CompletionEventSerializer<
                CompletedEvent = CE,
                Output = Vec<u8>,
                Error = crate::error::Error<Err>,
            > + Send
            + Sync
            + Clone
            + 'static,
    > FsCompletionHandler<Err, CE, EventSerializer>
{
    pub async fn mark_complete(
        &mut self,
        msg: FsEvent,
        completed: OutputEvent<CE, crate::error::Error<Err>>,
    ) {
        match completed.completed_event {
            Completion::Total(ce) => {
                info!("Marking all events complete - total success");
                self.completed_events.push(ce);
                self.completed_messages.push(msg);
                self.identities.extend(completed.identities);
            }
            Completion::Partial((ce, err)) => {
                warn!("EventHandler was only partially successful: {:?}", err);
                self.completed_events.push(ce);
                self.identities.extend(completed.identities);
            }
            Completion::Error(e) => {
                warn!("Event handler failed: {:?}", e);
            }
        };

        info!(
            "Marked event complete. {} completed events, {} completed messages",
            self.completed_events.len(),
            self.completed_messages.len(),
        );

        let cur = std::cmp::max(self.completed_events.len(), self.completed_messages.len());
        if cur > 100 {
            self.self_actor.clone().unwrap().ack_all(None).await;
        }
    }

    pub async fn ack_all(&mut self, notify: Option<tokio::sync::oneshot::Sender<()>>) {
        info!("Acking messages");
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
        let res = self.event_emitter.emit_event(serialized_event).await;
        res.expect("Failed to emit event");

        info!("Acked");

        self.completed_events.clear();
        self.completed_messages.clear();

        if let Some(notify) = notify {
            let _ = notify.send(());
        }
    }

    pub fn _p(&self, _p: std::marker::PhantomData<(Err, CE, EventSerializer)>) {}
}

#[async_trait]
impl<Err, CE, EventSerializer> CompletionHandler
    for FsCompletionHandlerActor<Err, CE, EventSerializer>
where
    Err: Debug + Send + Sync + 'static,
    CE: Send + Sync + Clone + 'static,
    EventSerializer: CompletionEventSerializer<
            CompletedEvent = CE,
            Output = Vec<u8>,
            Error = crate::error::Error<Err>,
        > + Send
        + Sync
        + Clone
        + 'static,
{
    type Message = FsEvent;
    type CompletedEvent = OutputEvent<CE, crate::error::Error<Err>>;

    async fn mark_complete(&self, msg: Self::Message, completed_event: Self::CompletedEvent) {
        FsCompletionHandlerActor::mark_complete(&self, msg, completed_event).await
    }

    async fn ack_all(&self, notify: Option<tokio::sync::oneshot::Sender<()>>) {
        FsCompletionHandlerActor::ack_all(&self, notify).await
    }

    async fn release(self) {
        FsCompletionHandlerActor::release(self).await
    }
}
