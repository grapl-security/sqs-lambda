use std::time::Duration;
use log::{info};
use crossbeam_channel::RecvTimeoutError;
use derive_aktor::derive_actor;
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher, Event};
use notify::event::{CreateKind, Event as FsEvent};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use async_trait::async_trait;

use crate::completion_event_serializer::CompletionEventSerializer;
use crate::completion_handler::CompletionHandler;
use crate::error::Error;
use crate::event_handler::OutputEvent;
use crate::event_processor::EventProcessorActor;
use crate::fs_completion_handler::FsCompletionHandlerActor;
use crate::consumer::Consumer;
use std::fmt::Debug;

pub struct FsNotifyConsumerHandler<Err, CE, EventSerializer>
    where
        Err: Debug + Send + Sync + Clone + 'static,
        CE: Send + Sync + Clone + 'static,
        EventSerializer: CompletionEventSerializer<CompletedEvent=CE, Output=Vec<u8>, Error=crate::error::Error<Err>> + Send + Sync + Clone + 'static,
          

{
    watcher: Option<RecommendedWatcher>,
    rx: crossbeam_channel::Receiver<FsEvent>,
    self_actor: Option<FsNotifyConsumerHandlerActor<Err, CE, EventSerializer>>,
    completion_handler: FsCompletionHandlerActor<Err, CE, EventSerializer>,
}

impl<Err, CE, EventSerializer> FsNotifyConsumerHandler<Err, CE, EventSerializer>
    where
        Err: Debug + Send + Sync + Clone + 'static,
        CE: Send + Sync + Clone + 'static,
        EventSerializer: CompletionEventSerializer<CompletedEvent=CE, Output=Vec<u8>, Error=crate::error::Error<Err>> + Send + Sync + Clone + 'static,
          
{
    pub fn new(
        directory: impl AsRef<str>,
        completion_handler: FsCompletionHandlerActor<Err, CE, EventSerializer>,
    ) -> Self {
        let directory = directory.as_ref();
        let (tx, rx) = crossbeam_channel::unbounded();

        let mut watcher: RecommendedWatcher = Watcher::new_immediate(
            move |res: Result<FsEvent, notify::Error>| tx.send(res.unwrap()).unwrap()
        ).expect("Watched failed to init");

        watcher.configure(Config::PreciseEvents(true))
            .expect(&format!("watcher.configure {}", directory));
        watcher.watch(directory, RecursiveMode::Recursive)
            .expect(&format!("watcher.watch {}", directory));

        Self {
            watcher: Some(watcher),
            rx,
            self_actor: None,
            completion_handler,
        }
    }
}

#[derive_actor]
impl<
    Err: Debug + Send + Sync + Clone + 'static,
    CE: Send + Sync + Clone + 'static,
    EventSerializer: CompletionEventSerializer<CompletedEvent=CE, Output=Vec<u8>, Error=crate::error::Error<Err>> + Send + Sync + Clone + 'static,
>
FsNotifyConsumerHandler<Err, CE, EventSerializer>
{
    pub async fn get_next_event(&mut self, event_processor: EventProcessorActor<FsEvent>) {
        info!("Received get_next_event request");
        match self.rx.recv_timeout(Duration::from_secs(2)) {
            Ok(event) => {
                match event.kind {
                    EventKind::Create(CreateKind::File) => {
                        info!("Sending a file create event");
                        event_processor.process_event(
                            event,
                        ).await;
                    }
                    e => {
                        info!("Received non-create event: {:?}", e);
                        self.self_actor.clone().unwrap().get_next_event(event_processor).await;
                    }
                }
            }
            Err(e) => {
                info!("Timeout, forcing ack");
                // self.watcher = None;
                let (tx, shutdown_notify) = tokio::sync::oneshot::channel();
                self.completion_handler.ack_all(Some(tx)).await;

                let _ = shutdown_notify.await;

                self.self_actor.clone().unwrap().get_next_event(event_processor).await;
                    // // event_processor.stop_processing().await;
                    // event_processor.release().await;
                    // self.completion_handler.clone().release().await;
                    // self.self_actor.clone().unwrap().release().await;

            }
        }
    }

    pub fn _phantom(&self, _p: std::marker::PhantomData<(Err, CE, EventSerializer)>) {}
}

#[async_trait]
impl<Err, CE, EventSerializer> Consumer<FsEvent> for FsNotifyConsumerHandlerActor<Err, CE, EventSerializer>
    where
        Err: Debug + Clone + Send + Sync + Clone + 'static,
        CE: Send + Sync + Clone + 'static,
        EventSerializer: CompletionEventSerializer<CompletedEvent=CE, Output=Vec<u8>, Error=crate::error::Error<Err>> + Send + Sync + Clone + 'static,

{
    async fn get_next_event(&self, event_processor: EventProcessorActor<Event>) {
        FsNotifyConsumerHandlerActor::get_next_event(
            self,
            event_processor
        ).await
    }
}