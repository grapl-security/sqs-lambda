use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::error::Error;

use futures::{ Poll, task};
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc::{channel, Receiver, Sender};
use rusoto_sqs::{ReceiveMessageRequest, Sqs};
use futures::Future;

use rusoto_sqs::Message as SqsMessage;
use lambda_runtime::Context;

use crate::event_processor::EventProcessorActor;
use std::time::Instant;

pub struct ConsumePolicy {
    deadline: i64,
    stop_at: Duration
}

impl ConsumePolicy {
    pub fn new(deadline: i64, stop_at: Duration) -> Self {
        Self {
            deadline, stop_at
        }
    }

    pub fn should_consume(&self) -> bool {
        let cur_secs = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs() as i64,
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };

        cur_secs <= self.deadline - (self.stop_at.as_millis() as i64)
    }
}

pub struct SqsConsumer<S>
    where S: Sqs + Clone + Send + 'static
{
    sqs_client: S,
    queue_url: String,
    stored_events: Vec<SqsMessage>,
    consume_policy: ConsumePolicy,
}

impl<S> SqsConsumer<S>
    where S: Sqs + Clone + Send + 'static
{
    pub fn new(
        sqs_client: S,
        queue_url: String,
        consume_policy: ConsumePolicy,
    ) -> SqsConsumer<S>
    where
        S: Sqs,
    {
        Self {
            sqs_client,
            queue_url,
            stored_events: Vec::with_capacity(20),
            consume_policy,
        }
    }
}

impl<S> SqsConsumer<S>
    where S: Sqs + Clone + Send + 'static
{
    pub fn get_new_event(&mut self, event_processor: EventProcessorActor) {
        let should_consume = self.consume_policy.should_consume();

        if self.stored_events.len() == 0 && should_consume {
            let new_events = self.batch_get_events().unwrap();
            self.stored_events.extend(new_events);
        }

        // What if we have no events? After a certain point the lambda should just shut down
        if let Some(next_event) = self.stored_events.pop() {
            event_processor.process_event(next_event);
        }
    }

    pub fn batch_get_events(&self) -> Result<Vec<SqsMessage>, Box<dyn Error>> {
        let recv = self.sqs_client.receive_message(
            ReceiveMessageRequest {
                max_number_of_messages: Some(20),
                queue_url: self.queue_url.clone(),
                wait_time_seconds: Some(1),
                ..Default::default()
            }
        ).sync()?;

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
    where S: Sqs + Clone + Send + 'static
{
    pub fn route_message(&mut self, msg: SqsConsumerMessage) {
        match msg {
            SqsConsumerMessage::get_new_event { event_processor } => {
                self.get_new_event(event_processor)
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
        where S: Sqs + Clone + Send + 'static
    {
        let (sender, receiver) = channel(0);

        tokio::spawn(SqsConsumerRouter {
            receiver,
            actor_impl,
        });
        Self { sender }
    }

    pub fn get_new_event(&self, event_processor: EventProcessorActor) {
        let msg = SqsConsumerMessage::get_new_event { event_processor };
        tokio::spawn(self.sender.clone().send(msg).map(|_| ()).map_err(|_| ()));
    }
}

pub struct SqsConsumerRouter<S>
    where S: Sqs + Clone + Send + 'static
{
    receiver: Receiver<SqsConsumerMessage>,
    actor_impl: SqsConsumer<S>,
}

impl<S> Future for SqsConsumerRouter<S>
    where S: Sqs + Clone + Send + 'static
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
