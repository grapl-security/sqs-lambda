use log::info;
use async_trait::async_trait;

use crate::event_emitter::EventEmitter;
use std::time::{SystemTime, UNIX_EPOCH};
use std::fs::File;
use std::io::Write;
use std::fmt::Debug;

fn time_based_key_fn() -> String {
    let cur_ms = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    let cur_day = cur_ms - (cur_ms % 86400);

    format!(
        "{}-{}-{}",
        cur_day, cur_ms, uuid::Uuid::new_v4()
    )
}

pub struct FsEventEmitter {
    directory: String
}

impl FsEventEmitter {
    pub fn new(directory: impl Into<String>) -> Self {
        let directory = directory.into();
        Self {
            directory
        }
    }
}

#[async_trait]
impl EventEmitter for FsEventEmitter
{
    type Event = Vec<u8>;
    type Error = crate::error::Error<()>;

    async fn emit_event(&mut self, completed_events: Vec<Self::Event>) -> Result<(), Self::Error> {
        info!("Writing out {} bytes", completed_events.len());
        for event in completed_events {
            let path = time_based_key_fn();
            let path = format!("{}{}", self.directory, &path);
            let mut file = File::create(&path).expect(&path);
            file.write_all(&event).expect(&path);
            file.flush().expect(&path);
        }

        Ok(())
    }
}