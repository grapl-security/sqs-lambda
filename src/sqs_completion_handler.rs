

use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::{task, Future, Poll};

use rusoto_sqs::Message as SqsMessage;
use rusoto_sqs::{Sqs, SqsClient, DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry};
use crate::event_emitter::EventEmitter;
use crate::completion_event_serializer::CompletionEventSerializer;
use std::time::{Duration, Instant};

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

pub struct SqsCompletionHandler<CP, CE, Payload, EE>
where
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload> + Send + Clone + 'static,
    Payload: Send + Clone + 'static,
    CE: Send + Clone + 'static,
    EE: EventEmitter<Event = Payload> + Send + Clone + 'static,
{
    sqs_client: SqsClient,
    queue_url: String,
    completed_events: Vec<CE>,
    completed_messages: Vec<SqsMessage>,
    completion_serializer: CP,
    event_emitter: EE,
    completion_policy: CompletionPolicy,
}

impl<CP, CE, Payload, EE> SqsCompletionHandler<CP, CE, Payload, EE>
where
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload> + Send + Clone + 'static,
    Payload: Send + Clone + 'static,
    CE: Send + Clone + 'static,
    EE: EventEmitter<Event = Payload> + Send + Clone + 'static,
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
impl<CP, CE, Payload, EE> SqsCompletionHandler<CP, CE, Payload, EE>
where
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload> + Send + Clone + 'static,
    Payload: Send + Clone + 'static,
    CE: Send + Clone + 'static,
    EE: EventEmitter<Event = Payload> + Send + Clone + 'static,
{
    pub fn mark_complete(&mut self, sqs_message: SqsMessage, completed: CE) {
        self.completed_events.push(completed);
        self.completed_messages.push(sqs_message);

        if self.completion_policy.should_flush(self.completed_events.len() as u16) {
            let serialized_event = self
                .completion_serializer
                .serialize_completed_events(&self.completed_events[..]);

            // TODO: Retry on failure
            self.event_emitter.emit_event(serialized_event).unwrap();

            self.ack_all();
            self.completion_policy.set_last_flush();
        }
    }
    pub fn ack_all(&mut self) {

        // TODO: Ack async
        for chunk in self.completed_messages.chunks(10) {
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
            );
        }

        self.completed_events.clear();
        self.completed_messages.clear();
    }
}

#[allow(non_camel_case_types)]
pub enum SqsCompletionHandlerMessage<CE>
where
    CE: Send + Clone + 'static,
{
    mark_complete { msg: SqsMessage, completed: CE },
    ack_all {},
}

impl<CP, CE, Payload, EE> SqsCompletionHandler<CP, CE, Payload, EE>
where
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload> + Send + Clone + 'static,
    Payload: Send + Clone + 'static,
    CE: Send + Clone + 'static,
    EE: EventEmitter<Event = Payload> + Send + Clone + 'static,
{
    pub fn route_message(&mut self, msg: SqsCompletionHandlerMessage<CE>) {
        match msg {
            SqsCompletionHandlerMessage::mark_complete { msg, completed } => {
                self.mark_complete(msg, completed)
            }
            SqsCompletionHandlerMessage::ack_all {} => self.ack_all(),
        };
    }
}

#[derive(Clone)]
pub struct SqsCompletionHandlerActor<CE>
where
    CE: Send + Clone + 'static,
{
    sender: Sender<SqsCompletionHandlerMessage<CE>>,
}

impl<CE> SqsCompletionHandlerActor<CE>
where
    CE: Send + Clone + 'static,
{
    pub fn new<CP, Payload, EE>(actor_impl: SqsCompletionHandler<CP, CE, Payload, EE>) -> Self
    where
        CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload>
            + Send
            + Clone
            + 'static,
        Payload: Send + Clone + 'static,
        EE: EventEmitter<Event = Payload> + Send + Clone + 'static,
    {
        let (sender, receiver) = channel(0);

        tokio::spawn(SqsCompletionHandlerRouter {
            receiver,
            actor_impl,
        });

        Self { sender }
    }

    pub fn mark_complete(&self, msg: SqsMessage, completed: CE) {
        let msg = SqsCompletionHandlerMessage::mark_complete { msg, completed };
        tokio::spawn(self.sender.clone().send(msg).map(|_| ()).map_err(|_| ()));
    }
    pub fn ack_all(&self) {
        let msg = SqsCompletionHandlerMessage::ack_all {};
        tokio::spawn(self.sender.clone().send(msg).map(|_| ()).map_err(|_| ()));
    }
}

pub struct SqsCompletionHandlerRouter<CP, CE, Payload, EE>
where
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload> + Send + Clone + 'static,
    Payload: Send + Clone + 'static,
    CE: Send + Clone + 'static,
    EE: EventEmitter<Event = Payload> + Send + Clone + 'static,
{
    receiver: Receiver<SqsCompletionHandlerMessage<CE>>,
    actor_impl: SqsCompletionHandler<CP, CE, Payload, EE>,
}

impl<CP, CE, Payload, EE> Future for SqsCompletionHandlerRouter<CP, CE, Payload, EE>
where
    CP: CompletionEventSerializer<CompletedEvent = CE, Output = Payload> + Send + Clone + 'static,
    Payload: Send + Clone + 'static,
    CE: Send + Clone + 'static,
    EE: EventEmitter<Event = Payload> + Send + Clone + 'static,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.receiver.poll() {
            Ok(futures::Async::Ready(Some(msg))) => {
                task::current().notify();
                self.actor_impl.route_message(msg);
                Ok(futures::Async::NotReady)
            }
            Ok(futures::Async::Ready(None)) => {
                self.receiver.close();
                Ok(futures::Async::Ready(()))
            }
            _ => Ok(futures::Async::NotReady),
        }
    }
}