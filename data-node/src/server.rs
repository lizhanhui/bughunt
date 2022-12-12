use crate::cfg::ServerConfig;
use async_channel::Sender;
use bytes::{BufMut, BytesMut};
use codec::frame::{Frame, OperationCode};
use monoio::{
    io::Splitable,
    net::{TcpListener, TcpStream},
};
use slog::{debug, error, info, o, warn, Drain, Logger};
use transport::connection::{ChannelReader, ChannelWriter};

/// Change it to `true` to verify things work without connection multiplexing
const IN_PLACE: bool = false;

pub fn launch(cfg: &ServerConfig) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    let core_ids = match core_affinity::get_core_ids() {
        Some(ids) => ids,
        None => {
            warn!(log, "No cores are available to set affinity");
            return;
        }
    };
    let available_core_len = core_ids.len();

    let handles = core_ids
        .into_iter()
        .skip(available_core_len - cfg.concurrency)
        .map(|core_id| {
            let server_config = cfg.clone();
            let logger = log.new(o!());
            std::thread::Builder::new()
                .name("Worker".to_owned())
                .spawn(move || {
                    monoio::utils::bind_to_cpu_set([core_id.id]).unwrap();
                    let mut driver = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_timer()
                        .with_entries(server_config.queue_depth)
                        .build()
                    {
                        Ok(driver) => driver,
                        Err(e) => {
                            error!(logger, "Failed to create runtime. Cause: {}", e.to_string());
                            panic!("Failed to create runtime driver. {}", e.to_string());
                        }
                    };

                    driver.block_on(async {
                        let bind_address = format!("0.0.0.0:{}", server_config.port);
                        let listener = match TcpListener::bind(&bind_address) {
                            Ok(listener) => {
                                info!(logger, "Server starts OK, listening {}", bind_address);
                                listener
                            }
                            Err(e) => {
                                eprintln!("{}", e.to_string());
                                return;
                            }
                        };

                        match run(listener, logger.new(o!())).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!(logger, "Runtime failed. Cause: {}", e.to_string());
                            }
                        }
                    });
                })
        })
        .collect::<Vec<_>>();

    for handle in handles.into_iter() {
        let _result = handle.unwrap().join();
    }
}

async fn run(listener: TcpListener, logger: Logger) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let incoming = listener.accept().await;
        let logger = logger.new(o!());
        let (stream, _socket_address) = match incoming {
            Ok((stream, socket_addr)) => {
                debug!(logger, "Accepted a new connection from {socket_addr:?}");
                stream.set_nodelay(true).unwrap_or_else(|e| {
                    warn!(logger, "Failed to disable Nagle's algorithm. Cause: {e:?}, PeerAddress: {socket_addr:?}");
                });
                debug!(logger, "Nagle's algorithm turned off");

                (stream, socket_addr)
            }
            Err(e) => {
                error!(
                    logger,
                    "Failed to accept a connection. Cause: {}",
                    e.to_string()
                );
                break;
            }
        };

        monoio::spawn(async move {
            process(stream, logger).await;
        });
    }

    Ok(())
}

async fn process(mut stream: TcpStream, logger: Logger) {
    let peer_address = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_e) => "Unknown".to_owned(),
    };

    let (read_half, write_half) = stream.split();
    let mut channel_reader = ChannelReader::new(read_half, &peer_address, logger.new(o!()));
    let mut channel_writer = ChannelWriter::new(write_half, &peer_address, logger.new(o!()));
    let (tx, rx) = async_channel::unbounded();

    loop {
        monoio::pin! {
            let read_fut = channel_reader.read_frame();
            let recv_fut = rx.recv();
        }

        monoio::select! {
            request = read_fut => {
                match request {
                    Ok(Some(frame)) => {
                        // No network multiplexing
                        if IN_PLACE {
                            debug!(logger, "Request[stream-id={}] received", frame.stream_id);
                            let mut header = BytesMut::new();
                            let text = format!("stream-id={}, response=true", frame.stream_id);
                            header.put(text.as_bytes());
                            let response = Frame {
                                operation_code: OperationCode::Ping,
                                flag: 1u8,
                                stream_id: frame.stream_id,
                                header_format: codec::frame::HeaderFormat::FlatBuffer,
                                header: Some(header.freeze()),
                                payload: None,
                            };
                            match channel_writer.write_frame(&response).await {
                                Ok(_) => {
                                    debug!(logger, "Response[stream-id={:?}] written to network", response.stream_id);
                                },
                                Err(e) => {
                                    warn!(
                                        logger,
                                        "Failed to write response[stream-id={:?}] to network. Cause: {:?}",
                                        response.stream_id,
                                        e
                                    );
                                }
                            };
                        } else { // Support TCP connection multiplexing
                            let log = logger.new(o!());
                            let sender = tx.clone();

                            let mut server_call = ServerCall {
                                request: frame, sender, logger: log
                            };
                            monoio::spawn(async move {
                                server_call.call().await;
                            });
                        }
                    }
                    Ok(None) => {
                        info!(
                            logger,
                            "Connection to {} is closed",
                            peer_address
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(
                            logger,
                            "Connection reset. Peer address: {}. Cause: {e:?}",
                            peer_address
                        );
                        break;
                    }
                }
            }
            response = recv_fut => {
                match response {
                    Ok(frame) => {
                        match channel_writer.write_frame(&frame).await {
                            Ok(_) => {
                                debug!(logger, "Response[stream-id={:?}] written to network", frame.stream_id);
                            },
                            Err(e) => {
                                warn!(
                                    logger,
                                    "Failed to write response[stream-id={:?}] to network. Cause: {:?}",
                                    frame.stream_id,
                                    e
                                );
                            }
                        }
                    },
                    Err(e) => {
                        warn!(logger, "Failed to receive response frame from MSPC channel. Cause: {:?}", e);
                    }
                };
            }
        }
    }
}

struct ServerCall {
    request: Frame,
    sender: Sender<Frame>,
    logger: Logger,
}

impl ServerCall {
    async fn call(&mut self) {
        match self.request.operation_code {
            OperationCode::Unknown => {}
            OperationCode::Ping => {
                debug!(
                    self.logger,
                    "Request[stream-id={}] received", self.request.stream_id
                );
                let mut header = BytesMut::new();
                let text = format!("stream-id={}, response=true", self.request.stream_id);
                header.put(text.as_bytes());
                let response = Frame {
                    operation_code: OperationCode::Ping,
                    flag: 1u8,
                    stream_id: self.request.stream_id,
                    header_format: codec::frame::HeaderFormat::FlatBuffer,
                    header: Some(header.freeze()),
                    payload: None,
                };
                match self.sender.send(response).await {
                    Ok(_) => {
                        debug!(
                            self.logger,
                            "Response[stream-id={}] transferred to channel", self.request.stream_id
                        );
                    }
                    Err(e) => {
                        warn!(
                            self.logger,
                            "Failed to send response[stream-id={}] to channel. Cause: {:?}",
                            self.request.stream_id,
                            e
                        );
                    }
                };
            }
            OperationCode::GoAway => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;

    #[test]
    fn test_core_affinity() {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let core_count = min(core_ids.len(), 2);

        let len = core_ids.len();
        let handles = core_ids
            .into_iter()
            .skip(len - core_count)
            .map(|processor_id| {
                std::thread::Builder::new()
                    .name("Worker".into())
                    .spawn(move || {
                        if core_affinity::set_for_current(processor_id) {
                            println!(
                                "Set affinity for worker thread {:?} OK",
                                std::thread::current()
                            );
                        }
                    })
            })
            .collect::<Vec<_>>();

        for handle in handles.into_iter() {
            handle.unwrap().join().unwrap();
        }
    }
}
