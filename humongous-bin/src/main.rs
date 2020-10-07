use futures::stream::StreamExt;
use futures::future::join_all;
use async_channel::unbounded;

use log::{trace, debug, info, warn, error};
use env_logger;

use humongous_lib::*;
use humongous_lib::http::get_compressed_warc_records;
use humongous_lib::conversions::{Lang, tag_language};
use humongous_lib::conversions::{http_response_body, html_to_text};

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let urls = vec![
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00000.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00001.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00002.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00003.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00004.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00005.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00006.warc.gz",
        "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2020-34/segments/1596439735792.85/warc/CC-MAIN-20200803083123-20200803113123-00007.warc.gz",
    ];

    const N_TAGGING_TASKS: usize = 8;
    const N_HTML_TO_TEXT_TASKS: usize = 8;

    let (warc_sender, warc_receiver) = unbounded::<Warc>();
    let (html_sender, html_receiver) = unbounded::<(Lang, String)>();
    let (tally_sender, tally_receiver) = unbounded::<()>();

    let download_handles = urls.iter().map(move |&url| { 
        let warc_sender = warc_sender.clone();
        tokio::spawn(async move {
            let stream_option = get_compressed_warc_records(url).await;
            match stream_option {
                Some(stream) => {
                    info!("Stream successfully connected from {:?}!", url);
                    stream.for_each_concurrent(None, |x| async {
                        match x {
                            WarcResult::Ok(wr) => {
                                warc_sender.send(wr).await;
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

    let process_handles = [..N_TAGGING_TASKS].iter().map(move |_| {
        let warc_receiver = warc_receiver.clone();
        let html_sender = html_sender.clone();

        tokio::spawn(async move {
            loop {
                match warc_receiver.recv().await {
                    Ok(warc) => {
                        if let Some(body) = http_response_body(warc) {
                            let lang = tag_language(&body, Some(128));
                            match lang {
                                Some(l) => {
                                    html_sender.send((l, body)).await;
                                },
                                _ => ()
                            }
                        };
                    },
                    Err(async_channel::RecvError) => {
                        break
                    },
                }
            }
            ()
        })
    });

    let html_to_text_handles = [..N_HTML_TO_TEXT_TASKS].iter().map(move |_| {
        let html_receiver = html_receiver.clone();
        let tally_sender = tally_sender.clone();

        tokio::spawn(async move {
            loop {
                match html_receiver.recv().await {
                    Ok((lang, html)) => {
                        html_to_text(html);
                        tally_sender.send(()).await;
                    },
                    Err(async_channel::RecvError) => {
                        break
                    },
                }
            }
            ()
        })
    });

    join_all(download_handles.chain(process_handles).chain(html_to_text_handles).chain(counter_handle)).await;

    
}
