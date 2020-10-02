use std::pin::Pin;
use std::task::{Context, Poll};
use std::io::{Error, ErrorKind};
use futures::stream::{self, Stream, StreamExt, BoxStream};
use async_compression::stream::GzipDecoder;
use std::io::Result;
use bytes::Bytes;
use nom::{Err, IResult, Needed};
use warc_parser::{record, records};
use reqwest::{get, Response};
use tokio::prelude::*;
use log::{debug, info, error};
use env_logger;

pub type Warc = warc_parser::Record;
pub struct WarcError();

pub enum MyResult<T, E> {
    Ok(T),
    Err(E),
}

pub enum WarcResult {
    WarcOk(Warc),
    WarcErr(WarcError),
}

impl From<Result<Bytes>> for MyResult<Bytes, ()> {
    fn from(item: Result<Bytes>) -> Self {
        match item {
            Ok(x) => {
                MyResult::Ok(x)
            },
            Err(_) => MyResult::Err(()),
        }
    }
}

impl From<reqwest::Result<Bytes>> for MyResult<Bytes, ()> {
    fn from(item: reqwest::Result<Bytes>) -> Self {
        match item {
            Ok(x) => {
                MyResult::Ok(x)
            },
            Err(_) => MyResult::Err(()),
        }
    }
}

pub struct WarcDecoder<'a> {
    stream: BoxStream<'a, MyResult<Bytes, ()>>,
    buffer: Vec<u8>,
    records: Vec<Warc>,
    stream_done: bool,
}

impl<'a, S> From<S> for WarcDecoder<'a> 
where S: 'a + Stream<Item = MyResult<Bytes, ()>> + std::marker::Send {
    fn from(stream: S) -> Self {
        return WarcDecoder {
            stream: stream.boxed(),
            buffer: Vec::new(),
            records: Vec::new(),
            stream_done: false,
        }
    }
}

impl Stream for WarcDecoder<'_> {
    type Item = WarcResult;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let me = Pin::into_inner(self);
        
        if !me.stream_done {
            loop {
                let polled = Pin::new(&mut me.stream).poll_next(cx);
                match polled {
                    // Bytestream is finished
                    Poll::Ready(None) => {
                        info!("Nothing left in byte stream!");
                        me.stream_done = true;
                        break;
                    },
                    // Bytestream yielded new bytes
                    Poll::Ready(Some(MyResult::Ok(bytes))) => {
                        let mut byte_vec: Vec<u8> = bytes.into_iter().collect();
                        //debug!("Got {:?} bytes!", byte_vec.len());
                        me.buffer.append(&mut byte_vec);
                    },
                    // Bytestream has an error
                    Poll::Ready(Some(_)) => {
                        error!("An error occurred!");
                        return Poll::Ready(Some(WarcResult::WarcErr(WarcError())))
                    },
                    // Bytestream not yet ready
                    Poll::Pending => break,
                }
            }
        }
        
        // Remove all bytes from the buffer and try to build a set of records from them
        let slice = me.buffer.split_off(0);
        match records(slice.as_slice()) {
            // If records can be parsed from the buffered bytes
            Ok((remainder, entries)) => {
                info!("{:?} records were parsed!", entries.len());
                //println!("{:?}", entries);
                // Add parsed records to the queue
                me.records.extend(entries);
                // Put remaining bytes back in the buffer
                me.buffer.extend_from_slice(remainder);
            },
            // Should deal with other cases explicitly, for error and incomplete cases
            _ => {
                me.buffer.extend(slice);
            },
        };

        /*
        // Remove all bytes from the buffer and try to build a single record from them
        let slice = me.buffer.split_off(0);
        match record(slice.as_slice()) {
            // If records can be parsed from the buffered bytes
            Ok((remainder, entry)) => {
                debug!("1 record was parsed!");
                // Add parsed record to the queue
                me.records.push(entry);
                // Put remaining bytes back in the buffer
                me.buffer.extend_from_slice(remainder);
            },
            // Should deal with other cases explicitly, for error and incomplete cases
            _ => {
                //debug!("No more records without more bytes!");
                me.buffer.extend(slice);
            },
        };
        */

        match me.records.pop() {
            // Take a record from the queue
            Some(record) => {
                return Poll::Ready(Some(WarcResult::WarcOk(record)));
            },
            // Nothing left in the record queue
            None => {
                // Nothing left in either the stream or the record queue
                if me.stream_done {
                    return Poll::Ready(None);
                } else {
                    return Poll::Pending;
                }
            },
        }
    }
}

pub async fn get_uncompressed_warc_records(url: &str) -> Option<WarcDecoder<'static>> {
    match reqwest::get(url).await {
        Ok(response) => {
            let stream = response.bytes_stream();
            let mapped_stream = stream.map(|x| x.into());
            return Some(WarcDecoder::from(mapped_stream));
        },
        Err(_) => return None
    }
}

pub async fn get_compressed_warc_records(url: &str) -> Option<WarcDecoder<'static>> {
    match reqwest::get(url).await {
        Ok(response) => {
            let stream = response.bytes_stream();
            let mut decoded_stream = GzipDecoder::new(stream.map(|x| {
                match x {
                    Ok(x) => Ok(x),
                    _ => {
                        error!("Error mapping from reqwest::Result to std::io::Result!");
                        Err(Error::new(ErrorKind::Other, "Decoding error."))
                    },
                }
            }));
            decoded_stream.multiple_members(true);
            let mapped_stream = decoded_stream.map(|x| x.into());         
            return Some(WarcDecoder::from(mapped_stream));
        },
        Err(_) => return None
    }
}

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let handle = tokio::spawn(async move {
        //let stream_option = get_uncompressed_warc_records("https://github.com/webrecorder/warcio/raw/master/test/data/example.warc").await;
        //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/plethora.warc").await;
        //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/bbc.warc").await;

        //let stream_option = get_compressed_warc_records("https://github.com/webrecorder/warcio/raw/master/test/data/example.warc.gz").await;
        //let stream_option = get_compressed_warc_records("https://archive.org/download/warc-www.hifimuseum.de-2018-11-26/www.hifimuseum.de_2018-11-26-00000.warc.gz").await;
        let stream_option = get_compressed_warc_records("https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735810.18/warc/CC-MAIN-20200803111838-20200803141838-00363.warc.gz").await;
        match stream_option {
            Some(stream) => {
                println!("Stream successfully connected!");
                //let count = stream.fuse().fold(0, |acc, x| async move { acc + 1 }).await;
                //println!("Number of WARC records: {:?}", count);
                let collection = stream.fuse().collect::<Vec<WarcResult>>().await;
                println!("Number of WARC records: {:?}", collection.len());
                return ();
            },
            None => {
                println!("Stream was not successfully connected.");
                return ();
            }
        }
    });

    handle.await;
}
