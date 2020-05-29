use std::error::Error;

use log::{info, warn};
use notify::Event as FsEvent;
use serde::export::PhantomData;

use async_trait::async_trait;

use crate::event_decoder::PayloadDecoder;
use crate::event_retriever::PayloadRetriever;

#[derive(Clone)]
pub struct FsRetriever<D, E>
where
    D: PayloadDecoder<E> + Clone + Send + 'static,
    E: Send + 'static,
{
    decoder: D,
    phantom: PhantomData<E>,
}

impl<D, E> FsRetriever<D, E>
where
    D: PayloadDecoder<E> + Clone + Send + 'static,
    E: Send + 'static,
{
    pub fn new(decoder: D) -> Self {
        Self {
            decoder,
            phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<D, E> PayloadRetriever<E> for FsRetriever<D, E>
where
    D: PayloadDecoder<E> + Clone + Send + 'static,
    E: Send + 'static,
{
    type Message = FsEvent;

    async fn retrieve_event(&mut self, msg: &Self::Message) -> Result<E, Box<dyn Error>> {
        if msg.paths.len() > 1 {
            warn!("Received multiple paths in this event. We only process one event at a time.");
        }
        info!("Got a new event");

        let event_path = &msg.paths[0];
        let encoded_event = std::fs::read(event_path)?;

        Ok(self.decoder.decode(encoded_event)?)
    }
}
