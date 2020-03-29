use std::sync::{Arc, Mutex};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub enum Error<PE>
    where PE: Debug + Clone + Send + Sync + 'static,
{
    CacheError(String),
    ProcessingError(PE),
    OnEmissionError(String),
    IoError(String),
    EncodeError(String),
    DecodeError(String),
}


impl<PE> From<PE> for Error<PE>
    where PE: Debug + Clone + Send + Sync + 'static,
{
    fn from(err: PE) -> Self {
        Self::ProcessingError(err)
    }
}
