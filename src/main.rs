
use std::collections::{HashMap, HashSet};
use bytes::Bytes;

use std::pin::Pin;
use std::task::{Context, Poll};
use std::io::{Error, ErrorKind};
use futures::stream::{self, Stream, StreamExt, BoxStream};
use async_compression::stream::GzipDecoder;
use std::io::Result;
use nom::{Err, IResult, Needed};
use warc_parser::{record, records};
use reqwest::{get, Response};
use futures::future::join_all;
use tokio::prelude::*;

use log::{trace, debug, info, warn, error};
use env_logger;
use bumpalo::Bump;

use whatlang::{detect_lang};
use httparse;
use chardet::charset2encoding;
use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use ammonia::Builder;

pub type Warc = warc_parser::Record;
pub struct WarcError();

pub enum MyResult<T, E> {
    Ok(T),
    Err(E),
}

pub enum WarcResult {
    Ok(Warc),
    Err(WarcError),
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
        
        // If inner stream has not yet ended
        if !me.stream_done {
            // Loop until we get pending or the end of stream
            loop {
                // Poll inner stream for current status
                let polled = Pin::new(&mut me.stream).poll_next(cx);
                match polled {
                    // Byte stream is finished
                    Poll::Ready(None) => {
                        info!("Nothing left in byte stream!");
                        me.stream_done = true;
                        break;
                    },
                    // Byte stream yielded new bytes
                    Poll::Ready(Some(MyResult::Ok(bytes))) => {
                        let mut byte_vec: Vec<u8> = bytes.into_iter().collect();
                        me.buffer.append(&mut byte_vec);
                    },
                    // Byte stream has an error
                    Poll::Ready(Some(_)) => {
                        error!("An error occurred!");
                        return Poll::Ready(Some(WarcResult::Err(WarcError())))
                    },
                    // Byte stream not yet ready
                    Poll::Pending => break,
                }
            }
        }
        
        // Remove all bytes from the buffer and try to build a set of records from them
        me.buffer = {
            let drain = me.buffer.drain(..);
            match records(drain.as_slice()) {
                // If records can be parsed from the buffered bytes
                Ok((remainder, entries)) => {
                    debug!("{:?} records were parsed!", entries.len());
                    // Add parsed records to the queue
                    me.records.extend(entries);
                    // Put remaining bytes back in the buffer
                    remainder.to_vec()
                },
                // Should deal with other cases explicitly, for error and incomplete cases
                _ => {
                    drain.collect()
                },
            }
        };

        match me.records.pop() {
            // Take a record from the queue
            Some(record) => {
                return Poll::Ready(Some(WarcResult::Ok(record)));
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

pub fn http_body(mut warc: Warc) -> String {
    // WARC-Type is required by the spec, so this will panic if it is not included
    let warc_type = warc.headers.get("WARC-Type").unwrap();
    if warc_type != "response" {
        return "".to_string();
    }
    let mut headers = [httparse::EMPTY_HEADER; 32];

    let body_index = match httparse::Response::new(&mut headers).parse(warc.content.as_slice()) {
        Ok(httparse::Status::Complete((body_index))) => {
            Some(body_index)
        },
        _ => {
            None
        },
    };
    if let Some(i) = body_index {
        let body = warc.content.split_off(i);

        let result = chardet::detect(body.as_slice());
        // result.0 Encode
        // result.1 Confidence
        // result.2 Language

        debug!("{:?}: {:?}", result.0, result.1);

        // decode file into utf-8
        let coder = encoding_from_whatwg_label(charset2encoding(&result.0));
        if coder.is_some() {
            match coder.unwrap().decode(&body, DecoderTrap::Ignore) {
                Ok(text) => return text.to_string(),
                _ => (),
            }
        }
    }
    return String::new();
}

pub fn tag_language(text: String) -> (String, Option<whatlang::Lang>) {
    let sanitized_text = ammonia::Builder::default().allowed_classes(HashMap::new()).tags(HashSet::new()).clean(&text).to_string();
    let language = {
        if sanitized_text.len() > 25 {
            let lang = detect_lang(&sanitized_text);
            info!("Language detected: {:?}", lang);
            lang
        } else {
            None
        }
    };
    return (text, language);
}

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let urls = vec![
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00000.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00001.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00002.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00003.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00004.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00005.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00006.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00007.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00008.warc.gz",
        //
        //"https://archive.org/download/warc-www.hifimuseum.de-2018-11-26/www.hifimuseum.de_2018-11-26-00000.warc.gz",
        //"https://github.com/webrecorder/warcio/raw/master/test/data/example.warc.gz",
    ];


    let handles = urls.iter().map(|&url| { 
        tokio::spawn(async move {
            //let stream_option = get_uncompressed_warc_records("https://github.com/webrecorder/warcio/raw/master/test/data/example.warc").await;
            //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/plethora.warc").await;
            //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/bbc.warc").await;

            let stream_option = get_compressed_warc_records(url).await;
            match stream_option {
                Some(stream) => {
                    info!("Stream successfully connected from {:?}!", url);
                    let count = stream.map(|x| {
                        match x {
                            WarcResult::Ok(x) => http_body(x),
                            _ => "".to_string(),
                        }
                    }).map(|x| tag_language(x)).fuse().fold(0u32, |acc, x| async move { acc + 1 }).await;
                    println!("Number of WARC records: {:?}", count);
                    //let collection = stream.fuse().collect::<Vec<WarcResult>>().await;
                    //info!("Number of WARC records from {:?}: {:?}", url, collection.len());
                    return ();
                },
                None => {
                    error!("Stream was not successfully connected.");
                    return ();
                }
            }
        })
    });

    join_all(handles).await;
}
