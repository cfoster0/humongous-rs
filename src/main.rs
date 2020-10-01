use std::pin::Pin;
use std::task::{Context, Poll};
use std::io::{Error, ErrorKind};
use futures::stream::{self, Stream, StreamExt, BoxStream};
use std::io::Result;
use bytes::Bytes;
use nom::{Err, IResult, Needed};
use warc_parser::{record};
use reqwest::{get, Response};
use tokio::prelude::*;

pub type Warc = warc_parser::Record;

pub struct WarcDecoder<'a> {
    stream: BoxStream<'a, reqwest::Result<Bytes>>,
    buffer: Vec<u8>,
    records: Vec<Warc>,
}

impl<'a, S> From<S> for WarcDecoder<'a> 
where S: 'a + Stream<Item = reqwest::Result<Bytes>> + std::marker::Send {
    fn from(stream: S) -> Self {
        return WarcDecoder {
            stream: stream.boxed(),
            buffer: Vec::new(),
            records: Vec::new()
        }
    }
}

pub enum WarcResult {
    WarcOk(Warc),
    WarcError,
}

impl Stream for WarcDecoder<'_> {
    type Item = WarcResult;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let me = Pin::into_inner(self);
        let polled = Pin::new(&mut me.stream).poll_next(cx);
        match polled {
            // Bytestream is finished
            Poll::Ready(None) => {
                if me.records.is_empty() {
                    println!("Nothing left!");
                    return Poll::Ready(None)
                }
            },
            // Bytestream yielded new bytes
            Poll::Ready(Some(Ok(bytes))) => {
                let mut byte_vec: Vec<u8> = bytes.into_iter().collect();
                println!("Got {:?} bytes!", byte_vec.len());
                me.buffer.append(&mut byte_vec);
            },
            // Bytestream has an error
            Poll::Ready(Some(_)) => {
                println!("An error occurred!");
                return Poll::Ready(Some(WarcResult::WarcError))
            },
            // Bytestream not yet ready
            Poll::Pending => ()
        }
        // Try to parse records from the byte buffer
        let mut slice = me.buffer.as_slice();
        loop {
            match record(slice) {
                Ok((remainder, entry)) => {
                    println!("A record was parsed!");
                    me.records.push(entry);
                    slice = remainder;
                },
                // Should deal with other cases explicitly, for error and incomplete cases
                _ => {
                    println!("No more records without more bytes!");
                    break;
                },
            }
        }
        
        match me.records.pop() {
            // Record has been fully parsed
            Some(record) => {
                println!("A record was returned!");
                return Poll::Ready(Some(WarcResult::WarcOk(record)))
            },
            None => {
                println!("Continuing!");
                return Poll::Pending;
            }
        }
    }
}

pub async fn get_warc_records(url: &str) -> Option<WarcDecoder<'static>> {
    match reqwest::get(url).await {
        Ok(response) => {
            return Some(WarcDecoder::from(response.bytes_stream()));
        },
        Err(_) => return None
    }
}

#[tokio::main]
pub async fn main() {
    let stream_option = get_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/plethora.warc").await;
    match stream_option {
        Some(stream) => {
            println!("Stream successfully connected!");
            let collection = stream.fuse().collect::<Vec<WarcResult>>().await;
            println!("{:?}", collection.len());
        },
        None => println!("Stream was not successfully connected.")
    }
}
