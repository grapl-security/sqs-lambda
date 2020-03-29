use std::error::Error;
use std::time::Duration;

use futures::compat::Future01CompatExt;
use log::*;
use rusoto_s3::{PutObjectRequest, S3};

use async_trait::async_trait;

#[async_trait]
pub trait EventEmitter {
    type Event;
    type Error: std::fmt::Debug;
    async fn emit_event(&mut self, completed_events: Vec<Self::Event>) -> Result<(), Self::Error>;
}
