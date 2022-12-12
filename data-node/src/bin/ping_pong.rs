use bytes::{BufMut, BytesMut};
use clap::Parser;
use codec::frame::{Frame, HeaderFormat, OperationCode};
use monoio::{io::Splitable, net::TcpStream};
use slog::{debug, error, info, o, warn, Drain, Logger};
use transport::connection::{ChannelReader, ChannelWriter};

#[monoio::main(timer_enabled = true, driver = "fusion")]
async fn main() {
    let args = Args::parse();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    launch(&args, log).await
}

pub const DEFAULT_DATA_NODE_PORT: u16 = 10911;

#[derive(Debug, Parser)]
#[clap(name = "ping-pong", author, version, about, long_about = None)]
struct Args {
    #[clap(name = "hostname", short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    #[clap(short, long, default_value_t = DEFAULT_DATA_NODE_PORT)]
    port: u16,
}

async fn launch(args: &Args, logger: Logger) {
    let connect = format!("{}:{}", args.host, args.port);
    info!(logger, "Start to connect to {connect}");

    let mut stream = match TcpStream::connect(&connect).await {
        Ok(stream) => {
            info!(logger, "Connected to {connect:?}");
            match stream.set_nodelay(true) {
                Ok(_) => {
                    debug!(logger, "Nagle's algorithm turned off");
                }
                Err(e) => {
                    error!(
                        logger,
                        "Failed to turn Nagle's algorithm off. Cause: {:?}", e
                    );
                }
            }

            stream
        }
        Err(e) => {
            error!(logger, "Failed to connect to {connect:?}. Cause: {e:#?}");
            return;
        }
    };

    let mut frame = Frame {
        operation_code: OperationCode::Ping,
        flag: 0u8,
        stream_id: 0,
        header_format: HeaderFormat::FlatBuffer,
        header: None,
        payload: None,
    };

    fill_header(&mut frame);

    let mut buffer = bytes::BytesMut::with_capacity(4 * 1024);
    frame.encode(&mut buffer).unwrap_or_else(|e| {
        error!(logger, "Failed to encode frame. Cause: {e:#?}");
    });

    let (read_half, write_half) = stream.split();
    let mut read_channel = ChannelReader::new(read_half, &connect, logger.new(o!()));
    let mut write_channel = ChannelWriter::new(write_half, &connect, logger.new(o!()));
    write_channel.write_frame(&frame).await.unwrap();
    let mut cnt = 0;
    debug!(logger, "{cnt} Ping");
    loop {
        match read_channel.read_frame().await {
            Ok(Some(mut frame)) => {
                debug!(logger, "{cnt} Pong received");
                cnt += 1;
                frame.stream_id = cnt;
                fill_header(&mut frame);

                monoio::time::sleep(std::time::Duration::from_millis(100)).await;

                if let Ok(_) = write_channel.write_frame(&frame).await {
                    debug!(logger, "{cnt} Ping sent");
                } else {
                    warn!(logger, "Failed to ping...");
                    return;
                }
            }
            Ok(None) => {
                info!(logger, "Connection closed");
                return;
            }
            Err(e) => {
                error!(logger, "Connection reset by peer. {e:?}");
                return;
            }
        }
    }
}

fn fill_header(frame: &mut Frame) {
    let mut header = BytesMut::new();
    let text = format!("stream-id={}", frame.stream_id);
    header.put(text.as_bytes());
    let header = header.freeze();
    frame.header = Some(header);
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut, BytesMut};

    #[test]
    fn test_bytes() {
        let mut buf = BytesMut::new();
        let text = format!("k={}", 1);
        buf.put(text.as_bytes());
        let buf = buf.freeze();
        assert_eq!(3, buf.len());
        assert_eq!(3, buf.remaining());
    }
}
