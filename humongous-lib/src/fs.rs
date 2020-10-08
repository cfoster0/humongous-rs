use super::*;

#[cfg(feature = "fs-tokio")]
pub mod fs_tokio {
    use super::*;
    use tokio::fs::File;
    use tokio::io::AsyncBufRead;
    use tokio::io::BufReader;
    use async_compression::tokio_02::bufread::GzipDecoder;

    pub struct WarcDecoder<R: AsyncBufRead> {
        reader: R,
        buffer: Vec<u8>,
        records: Vec<Warc>,
        reader_done: bool,
    }

    impl<R> From<R> for WarcDecoder<R>
    where R: AsyncBufRead {
        fn from(reader: R) -> Self {
            return WarcDecoder {
                reader: reader,
                buffer: Vec::new(),
                records: Vec::new(),
                reader_done: false,
            }
        }
    }

    pub async fn get_uncompressed_warc_records(path: &str) -> Option<WarcDecoder<BufReader<File>>> {
        match File::open(path).await {
            Ok(file) => {
                Some(WarcDecoder::from(BufReader::new(file)))
            },
            Err(_) => None
        }
    }

    pub async fn get_compressed_warc_records(path: &str) -> Option<WarcDecoder<BufReader<GzipDecoder<BufReader<File>>>>> {
        match File::open(path).await {
            Ok(file) => {
                let mut decoder = GzipDecoder::new(BufReader::new(file));
                decoder.multiple_members(true);
                Some(WarcDecoder::from(BufReader::new(decoder)))
            },
            Err(_) => None,
        }
    }

    impl<R> Stream for WarcDecoder<R> 
    where R: AsyncBufRead + std::marker::Unpin {
        type Item = WarcResult;
        
        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let me = Pin::into_inner(self);
            
            // If async reader has not yet ended
            if !me.reader_done {
                // Loop until we get pending or the end of file
                loop {
                    // Poll async reader for current status
                    let polled = Pin::new(&mut me.reader).poll_fill_buf(cx);
                    match polled {
                        // Async reader is finished
                        Poll::Ready(Ok([])) => {
                            info!("Nothing left in file bytes!");
                            me.reader_done = true;
                            break;
                        },
                        // Async reader yielded new bytes
                        Poll::Ready(Ok(buf)) => {
                            let length = buf.len();
                            me.buffer.extend_from_slice(buf);
                            Pin::new(&mut me.reader).consume(length);
                        },
                        // Async reader has an error
                        Poll::Ready(_) => {
                            error!("An error occurred!");
                            return Poll::Ready(Some(WarcResult::Err(WarcError())))
                        },
                        // Async reader not yet ready
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
                    // Nothing left in either the async reader or the record queue
                    if me.reader_done {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Pending;
                    }
                },
            }
        }
    }
}

#[cfg(feature = "fs-async-std")]
pub mod fs_async_std {
    use super::*;
    use async_std::fs::File;
    use futures::io::AsyncBufRead;
    use async_std::io::BufReader;
    use async_compression::futures::bufread::GzipDecoder;

    pub struct WarcDecoder<R: AsyncBufRead> {
        reader: R,
        buffer: Vec<u8>,
        records: Vec<Warc>,
        reader_done: bool,
    }

    impl<R> From<R> for WarcDecoder<R>
    where R: AsyncBufRead {
        fn from(reader: R) -> Self {
            return WarcDecoder {
                reader: reader,
                buffer: Vec::new(),
                records: Vec::new(),
                reader_done: false,
            }
        }
    }

    impl<R> Stream for WarcDecoder<R> 
    where R: AsyncBufRead + std::marker::Unpin {
        type Item = WarcResult;
        
        fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
            let me = Pin::into_inner(self);
            
            // If async reader has not yet ended
            if !me.reader_done {
                // Loop until we get pending or the end of file
                loop {
                    // Poll async reader for current status
                    let polled = Pin::new(&mut me.reader).poll_fill_buf(cx);
                    match polled {
                        // Async reader is finished
                        Poll::Ready(Ok([])) => {
                            info!("Nothing left in file bytes!");
                            me.reader_done = true;
                            break;
                        },
                        // Async reader yielded new bytes
                        Poll::Ready(Ok(buf)) => {
                            let length = buf.len();
                            me.buffer.extend_from_slice(buf);
                            Pin::new(&mut me.reader).consume(length);
                        },
                        // Async reader has an error
                        Poll::Ready(_) => {
                            error!("An error occurred!");
                            return Poll::Ready(Some(WarcResult::Err(WarcError())))
                        },
                        // Async reader not yet ready
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
                    // Nothing left in either the async reader or the record queue
                    if me.reader_done {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Pending;
                    }
                },
            }
        }
    }

    pub async fn get_uncompressed_warc_records(path: &str) -> Option<WarcDecoder<BufReader<File>>> {
        match File::open(path).await {
            Ok(file) => {
                Some(WarcDecoder::from(BufReader::new(file)))
            },
            Err(_) => None
        }
    }

    pub async fn get_compressed_warc_records(path: &str) -> Option<WarcDecoder<BufReader<GzipDecoder<BufReader<File>>>>> {
        match File::open(path).await {
            Ok(file) => {
                let mut decoder = GzipDecoder::new(BufReader::new(file));
                decoder.multiple_members(true);
                Some(WarcDecoder::from(BufReader::new(decoder)))
            },
            Err(_) => None,
        }
    }

}