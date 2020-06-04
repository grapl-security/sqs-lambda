use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use color_eyre::Report;

#[derive(Debug)]
pub enum Error<PE>
where
    PE: Debug + Send + Sync + 'static,
{
    CacheError(String),
    ProcessingError(PE),
    OnEmissionError(String),
    IoError(String),
    EncodeError(String),
    DecodeError(String),
}

impl<PE> From<PE> for Error<PE>
where
    PE: Debug + Send + Sync + 'static,
{
    fn from(err: PE) -> Self {
        Self::ProcessingError(err)
    }
}
