use std::pin::Pin;
use std::task::{Context, Poll};
use std::io::{Error, ErrorKind};
use futures::stream::{self, Stream, StreamExt, BoxStream};
use std::io::Result;
use bytes::Bytes;
use nom::{Err, IResult, Needed};
use warc_parser::{record, records};
use reqwest::{get, Response};
use tokio::prelude::*;

pub type Warc = warc_parser::Record;

pub enum WarcResult {
    WarcOk(Warc),
    WarcError,
}

pub struct UncompressedWarcDecoder<'a> {
    stream: BoxStream<'a, reqwest::Result<Bytes>>,
    buffer: Vec<u8>,
    records: Vec<Warc>,
    stream_done: bool,
}

impl<'a, S> From<S> for UncompressedWarcDecoder<'a> 
where S: 'a + Stream<Item = reqwest::Result<Bytes>> + std::marker::Send {
    fn from(stream: S) -> Self {
        return UncompressedWarcDecoder {
            stream: stream.boxed(),
            buffer: Vec::new(),
            records: Vec::new(),
            stream_done: false,
        }
    }
}

impl Stream for UncompressedWarcDecoder<'_> {
    type Item = WarcResult;
    
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let me = Pin::into_inner(self);
        
        if !me.stream_done {
            loop {
                let polled = Pin::new(&mut me.stream).poll_next(cx);
                match polled {
                    // Bytestream is finished
                    Poll::Ready(None) => {
                        println!("Nothing left in byte stream!");
                        me.stream_done = true;
                        break;
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
                    Poll::Pending => break,
                }
            }
        }
        
        // Remove all bytes from the buffer and try to build a set of records from them
        let slice = me.buffer.split_off(0);
        match records(slice.as_slice()) {
            // If records can be parsed from the buffered bytes
            Ok((remainder, entries)) => {
                println!("{:?} records were parsed!", entries.len());
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

        // Remove all bytes from the buffer and try to build a single record from them
        let slice = me.buffer.split_off(0);
        match record(slice.as_slice()) {
            // If records can be parsed from the buffered bytes
            Ok((remainder, entry)) => {
                println!("1 record was parsed!");
                // Add parsed record to the queue
                me.records.push(entry);
                // Put remaining bytes back in the buffer
                me.buffer.extend_from_slice(remainder);
            },
            // Should deal with other cases explicitly, for error and incomplete cases
            _ => {
                println!("No more records without more bytes!");
                me.buffer.extend(slice);
            },
        };

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

pub async fn get_uncompressed_warc_records(url: &str) -> Option<UncompressedWarcDecoder<'static>> {
    match reqwest::get(url).await {
        Ok(response) => {
            return Some(UncompressedWarcDecoder::from(response.bytes_stream()));
        },
        Err(_) => return None
    }
}

pub struct CompressedWarcDecoder{}


pub async fn get_compressed_warc_records(url: &str) -> Option<CompressedWarcDecoder> {
    match reqwest::get(url).await {
        Ok(response) => {
            unimplemented!();
        },
        Err(_) => return None
    }
}

#[tokio::main]
pub async fn main() {
    //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/plethora.warc").await;
    //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/bbc.warc").await;
    let stream_option = get_uncompressed_warc_records("https://github.com/webrecorder/warcio/raw/master/test/data/example.warc").await;

    // DON'T WORK
    //let stream_option = get_compressed_warc_records("https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735810.18/crawldiagnostics/CC-MAIN-20200803111838-20200803141838-00347.warc.gz").await;
    //let stream_option = get_compressed_warc_records("https://github.com/webrecorder/warcio/raw/master/test/data/example.warc.gz").await;
    //let stream_option = get_compressed_warc_records("https://archive.org/download/warc-www.hifimuseum.de-2018-11-26/www.hifimuseum.de_2018-11-26-00000.warc.gz").await;
    match stream_option {
        Some(stream) => {
            println!("Stream successfully connected!");
            let collection = stream.fuse().collect::<Vec<WarcResult>>().await;
            println!("Number of WARC records: {:?}", collection.len());
        },
        None => println!("Stream was not successfully connected.")
    }
}
