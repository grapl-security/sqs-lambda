use std::time::{SystemTime, UNIX_EPOCH, Duration};
use std::error::Error;

use futures::sink::Sink;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use rusoto_sqs::{ReceiveMessageRequest, Sqs};

use rusoto_sqs::Message as SqsMessage;

use crate::event_processor::EventProcessorActor;
use std::time::Instant;

use futures::{Future, FutureExt};
use futures::task::{Poll, Context};
use std::pin::Pin;

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
    shutdown_subscriber: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<S> SqsConsumer<S>
    where S: Sqs + Clone + Send + 'static
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
    where S: Sqs + Clone + Send + 'static
{
    pub async fn get_new_event(&mut self, event_processor: EventProcessorActor) {
        let should_consume = self.consume_policy.should_consume();

        if self.stored_events.len() == 0 && should_consume {
            let new_events = self.batch_get_events().unwrap();
            self.stored_events.extend(new_events);
        }

        if self.stored_events.is_empty() && !should_consume {
            let shutdown_subscriber = std::mem::replace(&mut self.shutdown_subscriber, None);
            match shutdown_subscriber {
                Some(shutdown_subscriber) => shutdown_subscriber.send(()).unwrap(),
                None => panic!("Attempted to shut down with empty shutdown_subscriber")
            }
        }

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
    pub async fn route_message(&mut self, msg: SqsConsumerMessage) {
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
        where S: Sqs + Clone + Send + 'static
    {
        let (sender, receiver) = channel(0);

        tokio::task::spawn(SqsConsumerRouter {
            receiver,
            actor_impl,
        });
        Self { sender }
    }

    pub async fn get_new_event(&self, event_processor: EventProcessorActor) {
        let msg = SqsConsumerMessage::get_new_event { event_processor };
        self.sender.clone().send(msg).await.map(|_| ()).map_err(|_| ());
    }
}

use futures::stream::Stream;
use pin_utils::unsafe_pinned;



use std::marker::Unpin;

pub struct SqsConsumerRouter<S>
    where S: Sqs + Clone + Send + 'static
{
    receiver: tokio::sync::mpsc::Receiver<SqsConsumerMessage>,
    actor_impl: SqsConsumer<S>,
}

use pin_utils::pin_mut;

impl<S> Future for SqsConsumerRouter<S>
    where S: Sqs + Clone + Send + 'static
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let received = {
            let mut receiver = unsafe {
                self.as_mut().map_unchecked_mut(|me| {
                    &mut me.receiver
                })
            };

            let received = receiver.poll_recv(cx);
            received
        };


        match received {
            Poll::Ready(Some(msg)) => {
                cx.waker().wake_by_ref();

                // route_message is async
                self.actor_impl.route_message(msg);

                Poll::Pending
            }
            Poll::Ready(None) => {
                unsafe {
                    self.as_mut().map_unchecked_mut(|me| {
                        me.receiver.close();
                        &mut me.receiver
                    })
                };
                Poll::Ready(())
            }
            _ => Poll::Pending,
        }
    }
}
