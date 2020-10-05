
use std::collections::{HashMap, HashSet};
use bytes::Bytes;
use std::cmp::min;
use maplit::{hashmap, hashset};

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
use async_channel::unbounded;

use log::{trace, debug, info, warn, error};
use env_logger;
use bumpalo::Bump;

use whatlang::{detect_lang};
use httparse;
use chardetng::EncodingDetector;
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

pub fn http_response_body(mut warc: Warc) -> Option<String> {
    // WARC-Type is required by the spec, so this will panic if it is not included
    let warc_type = warc.headers.get("WARC-Type").unwrap();
    if warc_type != "response" {
        return None;
    }
    // For WARC sets other than Common Crawl, consider filtering by "Content-Type".
    // Common Crawl already restricts itself to HTML files, but others (such as IA) don't.
    let mut headers = [httparse::EMPTY_HEADER; 16];

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

        let mut encoding_detector = EncodingDetector::new();
        // To check the encoding using the entire record (instead
        // of just a portion of the buffer), comment out the two lines
        // below and uncomment the line after it.
        let slice_to = min(body.len(), 1024);
        let not_ascii = encoding_detector.feed(&body[..slice_to], false);
        //let not_ascii = encoding_detector.feed(body.as_slice(), true);
        let char_encoding = encoding_detector.guess(None, true);
        let (cow, true_encoding, malformed) = char_encoding.decode(&body);
        debug!("{:?}", true_encoding);
        let text = cow.into_owned();
        //trace!("{:?}", text);
        return Some(text);
    } else {
        return None;
    }
}

pub fn tag_language(text: &str) -> Option<whatlang::Lang> {
    let char_count = text.chars().count();
    let take_to = min(char_count, 1024);
    let subset = text.chars().take(take_to).collect::<String>();
    let sanitized_subset = ammonia::Builder::default().allowed_classes(HashMap::new())
                                                    .tags(HashSet::new())
                                                    .generic_attributes(HashSet::new())
                                                    .clean(&subset)
                                                    .to_string();
    let language = {
        if sanitized_subset.len() > 25 {
            let lang = detect_lang(&sanitized_subset);
            debug!("Language detected: {:?}", lang);
            lang
        } else {
            None
        }
    };
    return language;
}

pub fn html_to_text(text: String) -> String {
    return text;
}

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let urls = vec![
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00000.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00001.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00002.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00003.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00004.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00005.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00006.warc.gz",
        //"https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00007.warc.gz",
        //
        //"https://archive.org/download/warc-www.hifimuseum.de-2018-11-26/www.hifimuseum.de_2018-11-26-00000.warc.gz",
        //"https://github.com/webrecorder/warcio/raw/master/test/data/example.warc.gz",
    ];

    const N_PROCESSOR_TASKS: usize = 8;

    let (sender, receiver) = unbounded::<Warc>();

    let (tally_sender, tally_receiver) = unbounded::<()>();

    let process_handles = [..N_PROCESSOR_TASKS].iter().map(move |_| {
        let receiver = receiver.clone();
        let tally_sender = tally_sender.clone();
        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(warc) => {
                        if let Some(body) = http_response_body(warc) {
                            let lang = tag_language(&body);
                            html_to_text(body);
                        };
                        tally_sender.send(()).await;
                        ()
                    },
                    Err(async_channel::RecvError) => {
                        break
                    },
                }
            }
            ()
        })
    });

    let download_handles = urls.iter().map(move |&url| { 
        let sender = sender.clone();
        tokio::spawn(async move {
            //let stream_option = get_uncompressed_warc_records("https://github.com/webrecorder/warcio/raw/master/test/data/example.warc").await;
            //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/plethora.warc").await;
            //let stream_option = get_uncompressed_warc_records("https://raw.githubusercontent.com/sbeckeriv/warc_nom_parser/master/sample/bbc.warc").await;

            let stream_option = get_compressed_warc_records(url).await;
            match stream_option {
                Some(stream) => {
                    info!("Stream successfully connected from {:?}!", url);
                    stream.for_each_concurrent(None, |x| async {
                        match x {
                            WarcResult::Ok(wr) => {
                                sender.send(wr).await;
                                ()
                            },
                            _ => (),
                        }
                    }).await;
                    return ();
                },
                None => {
                    error!("Stream was not successfully connected.");
                    return ();
                }
            }
        })
    });

    let counter_handle = [..1].iter().map(move |_| {
        let tally_receiver = tally_receiver.clone();
        tokio::spawn(async move {
            let mut tally = 0;
            loop {
                match tally_receiver.recv().await {
                    Ok(_) => {
                        tally = tally + 1;
                    },
                    Err(async_channel::RecvError) => {
                        info!("No more records, as all counter channel senders have been dropped!");
                        break;
                    },
                }
            }
            
            info!("Final size of WARC record collection: {:?}", tally);
        })
    });

    join_all(download_handles.chain(process_handles).chain(counter_handle)).await;

    
}
