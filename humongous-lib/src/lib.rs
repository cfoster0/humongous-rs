use bytes::Bytes;
use std::error::Error;
use std::io::ErrorKind;
use warc_parser::{Record, records};
use log::{trace, debug, info, warn, error};
use std::task::{Context, Poll};
use futures::stream::{Stream, StreamExt, BoxStream};
use std::pin::Pin;

pub mod http;
pub mod conversions;

pub type Warc = Record;
pub struct WarcError();

pub enum HResult<T, E> {
    Ok(T),
    Err(E),
}

pub enum WarcResult {
    Ok(Warc),
    Err(WarcError),
}

impl<E> From<Result<Bytes, E>> for HResult<Bytes, ()> {
    fn from(item: Result<Bytes, E>) -> Self {
        match item {
            Ok(x) => {
                HResult::Ok(x)
            },
            Err(_) => HResult::Err(()),
        }
    }
}

pub type LOLResult = Result<(), Box<dyn Error>>;