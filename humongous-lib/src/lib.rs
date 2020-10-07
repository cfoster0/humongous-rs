use bytes::Bytes;
use std::error::Error;
use std::io::ErrorKind;
use warc_parser::{Record, records};
use log::{trace, debug, info, warn, error};
use std::task::{Context, Poll};
use futures::stream::{Stream, StreamExt, BoxStream};
use std::pin::Pin;

pub type Warc = Record;
pub struct WarcError();

pub enum MyResult<T, E> {
    Ok(T),
    Err(E),
}

pub enum WarcResult {
    Ok(Warc),
    Err(WarcError),
}

impl<E> From<Result<Bytes, E>> for MyResult<Bytes, ()> {
    fn from(item: Result<Bytes, E>) -> Self {
        match item {
            Ok(x) => {
                MyResult::Ok(x)
            },
            Err(_) => MyResult::Err(()),
        }
    }
}

pub type LOLResult = Result<(), Box<dyn Error>>;

pub mod stream {
    use super::*;
    use async_compression::stream::GzipDecoder;

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
                            Err(std::io::Error::new(ErrorKind::Other, "Decoding error."))
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

}

pub mod conversions {
    use super::*;

    use std::cmp::min;
    use regex::Regex;
    use std::collections::{HashMap, HashSet};
    use whatlang::detect;
    use httparse;
    use chardetng::EncodingDetector;
    use lol_html::{rewrite_str, RewriteStrSettings, ElementContentHandlers, Selector};
    use lol_html::html_content::{Element, Comment, TextChunk};

    pub type Lang = whatlang::Lang;

    pub mod http {
        use super::*;

        pub fn http_response_body(mut warc: Warc) -> Option<String> {
            // WARC-Type is required by the spec, so this will panic if it is not included
            let warc_type = warc.headers.get("WARC-Type").unwrap();
            if warc_type != "response" {
                debug!("WARC-Type was not response. Skipping.");
                return None;
            }
            // For WARC sets other than Common Crawl, consider filtering by "Content-Type".
            // Common Crawl already restricts itself to HTML files, but others (such as IA) don't.
            let mut headers = [httparse::EMPTY_HEADER; 32];

            let body_index = match httparse::Response::new(&mut headers).parse(warc.content.as_slice()) {
                Ok(httparse::Status::Complete(body_index)) => {
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
                let _ = encoding_detector.feed(&body[..slice_to], false);
                //let _ = encoding_detector.feed(body.as_slice(), true);
                let char_encoding = encoding_detector.guess(None, true);
                let (cow, true_encoding, _malformed) = char_encoding.decode(&body);
                debug!("{:?}", true_encoding);
                let text = cow.into_owned();
                return Some(text);
            } else {
                return None;
            }
        }
    }

    pub mod html {
        use super::*;

        fn remove_attributes(el: &mut Element) -> LOLResult {
            let attribute_names: Vec<String> = el.attributes().iter().map(|att| att.name()).collect();
                
            for attribute_name in attribute_names {
                el.remove_attribute(&attribute_name);
            }
            return Ok(());
        }

        fn remove_html_element(el: &mut Element) -> LOLResult {
            el.remove();
            return Ok(());
        }

        fn remove_html_wrapper(el: &mut Element) -> LOLResult {
            el.remove_and_keep_content();
            return Ok(());
        }

        fn rename_to_block(el: &mut Element) -> LOLResult {
            match el.set_tag_name("block") {
                Ok(_) => return Ok(()),
                Err(_) => return Err(Box::new(core::fmt::Error)),
            }
        }

        fn rename_to_list(el: &mut Element) -> LOLResult {
            match el.set_tag_name("list") {
                Ok(_) => return Ok(()),
                Err(_) => return Err(Box::new(core::fmt::Error)),
            }
        }

        fn rename_to_item(el: &mut Element) -> LOLResult {
            match el.set_tag_name("item") {
                Ok(_) => return Ok(()),
                Err(_) => return Err(Box::new(core::fmt::Error)),
            }
        }

        fn remove_comment(com: &mut Comment) -> LOLResult {
            com.remove();
            return Ok(());
        }

        fn mark_block_ending(el: &mut Element) -> LOLResult {
            el.append("<br/>", lol_html::html_content::ContentType::Html);
            el.remove_and_keep_content();
            return Ok(());
        }

        fn remove_spacing(tc: &mut TextChunk) -> LOLResult {
            let shortened = tc.as_str().replace("\r\n", "").replace("\t", "").replace("\n", "").replace("  ", "");
            tc.replace(&shortened, lol_html::html_content::ContentType::Text);
            
            return Ok(());
        }

        fn outer_selectors_and_fns() -> Vec<(Selector, ElementContentHandlers<'static>)> {
            let all_selector: Selector = "*".parse().unwrap();
            let block_selector: Selector = "*".parse().unwrap();
            let li_selector: Selector = "li".parse().unwrap();
            let comment_selector: Selector = "*".parse().unwrap();

            let list_types = [
                                "ul",
                                "ol",
                                "dl",
                                ];

            let list_selectors = list_types.iter().map(|s| {
                let sel: Selector = s.parse().unwrap();
                return sel;
            });

            let list_handlers = list_selectors.map(|selector| {
                (selector, ElementContentHandlers::default().element(rename_to_list))
            });

            let unwanted_types = [
                                    "applet",
                                    "audio",
                                    "base",
                                    "basefont",
                                    "button", // ?
                                    "canvas",
                                    "datalist", // ?
                                    "embed",
                                    "figcaption",
                                    "figure",
                                    "frame",
                                    "frameset",
                                    "iframe",
                                    "head",
                                    "img",
                                    "input", // ?
                                    "link",
                                    "map",
                                    "meta",
                                    "nav",
                                    "noframes",
                                    "noscript",
                                    "object",
                                    "param",
                                    "progress",
                                    "script",
                                    "select", // ?
                                    "style",
                                    "source",
                                    "svg",
                                    "video",
                                    "wbr",
                                    ];

            let unwanted_selectors = unwanted_types.iter().map(|s| {
                let sel: Selector = s.parse().unwrap();
                return sel;
            });

            let unwanted_handlers = unwanted_selectors.map(|selector| {
                (selector, ElementContentHandlers::default().element(remove_html_element))
            });

            let block_types = [
                                "blockquote",
                                "caption",
                                "center",
                                "col",
                                "colgroup",
                                "dd",
                                "div",
                                "fieldset",
                                "form",
                                "h1",
                                "h2",
                                "h3",
                                "h4",
                                "h5",
                                "h6",
                                "legend",
                                "li",
                                "optgroup",
                                "option",
                                "p",
                                "pre",
                                "table",
                                "td",
                                "textarea",
                                "tfoot",
                                "th",
                                "thead",
                                "tr",
                                "ul"
                                ];

            let block_selectors = block_types.iter().map(|s| {
                let sel: Selector = s.parse().unwrap();
                return sel;
            });

            let block_handlers = block_selectors.map(|selector| {
                (selector, ElementContentHandlers::default().element(rename_to_block))
            });

            let unwrap_types = [
                                "abbr",
                                "acronym",
                                "address",
                                "article",
                                "aside",
                                "b",
                                "big",
                                "cite",
                                "code",
                                "data",
                                "details",
                                "dfn",
                                "em",
                                "font",
                                "footer",
                                "header",
                                "i",
                                "ins",
                                "kbd",
                                "label",
                                "pre",
                                "samp",
                                "section",
                                "small",
                                "span",
                                "summary",
                                "strike",
                                "strong",
                                "sub",
                                "sup",
                                "time",
                                "u",
                                "var",
                                ];

            let unwrap_selectors = unwrap_types.iter().map(|s| {
                let sel: Selector = s.parse().unwrap();
                return sel;
            });

            let unwrap_handlers = unwrap_selectors.map(|selector| {
                (selector, ElementContentHandlers::default().element(remove_html_wrapper))
            });

            let mut handlers = vec![
                (all_selector, ElementContentHandlers::default().element(remove_attributes)),
                (li_selector, ElementContentHandlers::default().element(rename_to_item)),
                (block_selector, ElementContentHandlers::default().text(remove_spacing)),
                //(a_selector, ElementContentHandlers::default().element(remove_html_element)),
                //(div_selector, ElementContentHandlers::default().element(remove_html_wrapper)),
                (comment_selector, ElementContentHandlers::default().comments(remove_comment)),
            ];
            handlers.extend(unwanted_handlers);
            handlers.extend(list_handlers);
            handlers.extend(unwrap_handlers);
            handlers.extend(block_handlers);
            handlers
        }

        fn inner_selectors_and_fns() -> Vec<(Selector, ElementContentHandlers<'static>)> {
            let block_selector: Selector = "block".parse().unwrap();

            let handlers = vec![
                                    (block_selector, ElementContentHandlers::default().element(mark_block_ending)),
                                    ];
            handlers
        }

        fn remove_link_lists(text: String) -> String {
            let list_of_links = Regex::new("((<a>.*</a>)(<a>.*</a>)+)").unwrap();
            return list_of_links.replace(&text, " ").to_string();
        }

        fn translate_breaks(text: String) -> String {
            let list_of_breaks = Regex::new("(<br/>)(<br/>)*").unwrap();
            return list_of_breaks.replace(&text, "\n").to_string();  
        }

        fn remove_tags(text: String) -> String {
            let sanitized = ammonia::Builder::default().allowed_classes(HashMap::new())
                                                        .tags(HashSet::new())
                                                        .generic_attributes(HashSet::new())
                                                        .clean(&text)
                                                        .to_string();
            return sanitized;
        }

        pub fn html_to_text(text: String) -> Option<String> {
            let (outer_selector_vec, outer_handler_vec): (Vec<Selector>, Vec<ElementContentHandlers>) = outer_selectors_and_fns().into_iter().unzip();
            let outer_handlers = outer_selector_vec.iter().zip(outer_handler_vec.into_iter()).collect();
            let outer_settings = RewriteStrSettings {
                element_content_handlers: outer_handlers,
                ..RewriteStrSettings::default()
            };

            let (inner_selector_vec, inner_handler_vec): (Vec<Selector>, Vec<ElementContentHandlers>) = inner_selectors_and_fns().into_iter().unzip();
            let inner_handlers = inner_selector_vec.iter().zip(inner_handler_vec.into_iter()).collect();
            let inner_settings = RewriteStrSettings {
                element_content_handlers: inner_handlers,
                ..RewriteStrSettings::default()
            };

            match rewrite_str(&text, outer_settings) {
                Ok(outer_text) => {
                    match rewrite_str(&outer_text, inner_settings) {
                        Ok(inner_text) => {
                            let rewritten = remove_tags(remove_link_lists(translate_breaks(inner_text)));
                            trace!("HTML-to-Text: {:?}", rewritten);
                            Some(rewritten)
                        },
                        _ => None,
                    }
                },
                _ => None,
            }

        }
    }
    

    pub fn tag_language(text: &str, min_length: Option<usize>) -> Option<whatlang::Lang> {
        let char_count = text.chars().count();
        let take_to = min(char_count, 4096);
        let subset = text.chars().take(take_to).collect::<String>();
        let sanitized_subset = ammonia::Builder::default().allowed_classes(HashMap::new())
                                                        .tags(HashSet::new())
                                                        .generic_attributes(HashSet::new())
                                                        .clean(&subset)
                                                        .to_string();
        let language = {
            match min_length {
                Some(ml) => {
                    if sanitized_subset.len() < ml {
                        return None;
                    }
                },
                _ => (),
            };

            let info = detect(&sanitized_subset)?;
            let lang = info.lang();
            if info.is_reliable() {
                debug!("Language detected: {:?}", lang);
                Some(lang)
            } else {
                None
            }
        };
        return language;
    }

    
}