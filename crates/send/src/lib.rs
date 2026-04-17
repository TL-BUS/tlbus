use std::collections::BTreeMap;
use std::path::PathBuf;

use tlbus_core::{BusError, Envelope, Result, write_envelope};
use tokio::net::UnixStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendConfig {
    pub socket_path: PathBuf,
    pub from: String,
    pub to: String,
    pub ttl_ms: u64,
    pub headers: BTreeMap<String, String>,
    pub payload: Vec<u8>,
}

impl SendConfig {
    pub fn parse_from<I, S>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut socket_path = PathBuf::from("/run/tlb.sock");
        let mut from = None;
        let mut to = None;
        let mut ttl_ms = 1_000_u64;
        let mut headers = BTreeMap::new();
        let mut payload = None;

        let mut args = args.into_iter().map(Into::into);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--socket" => {
                    let value = next_value(&mut args, "--socket")?;
                    socket_path = PathBuf::from(value);
                }
                "--from" => {
                    from = Some(next_value(&mut args, "--from")?);
                }
                "--to" => {
                    to = Some(next_value(&mut args, "--to")?);
                }
                "--ttl-ms" => {
                    let value = next_value(&mut args, "--ttl-ms")?;
                    ttl_ms = value.parse().map_err(|_| {
                        BusError::Configuration(format!(
                            "invalid `--ttl-ms` value `{value}`: expected an integer"
                        ))
                    })?;
                }
                "--header" => {
                    let value = next_value(&mut args, "--header")?;
                    let (key, header_value) = parse_header_arg(&value)?;
                    headers.insert(key, header_value);
                }
                "--protocol" => {
                    let value = next_value(&mut args, "--protocol")?;
                    headers.insert("protocol".to_string(), value);
                }
                "--transport" => {
                    let value = next_value(&mut args, "--transport")?;
                    headers.insert("transport".to_string(), value);
                }
                "--protocol-version" => {
                    let value = next_value(&mut args, "--protocol-version")?;
                    headers.insert("protocol_version".to_string(), value);
                }
                "--content-type" => {
                    let value = next_value(&mut args, "--content-type")?;
                    headers.insert("content_type".to_string(), value);
                }
                "--payload" => {
                    payload = Some(next_value(&mut args, "--payload")?.into_bytes());
                }
                "-h" | "--help" => {
                    return Err(BusError::Configuration("help requested".to_string()));
                }
                _ => {
                    return Err(BusError::Configuration(format!("unknown argument `{arg}`")));
                }
            }
        }

        Ok(Self {
            socket_path,
            from: require_value(from, "--from")?,
            to: require_value(to, "--to")?,
            ttl_ms,
            headers,
            payload: require_value(payload, "--payload")?,
        })
    }

    pub fn into_envelope(self) -> Envelope {
        let mut envelope = Envelope::new(self.from, self.to, self.payload);
        envelope.ttl_ms = self.ttl_ms;
        envelope.headers = self.headers;
        // The sender defaults to `raw` semantics so a remote bridge always receives a
        // protocol hint even for opaque payloads.
        envelope
            .headers
            .entry("protocol".to_string())
            .or_insert_with(|| "raw".to_string());
        envelope
    }
}

pub async fn send(config: SendConfig) -> Result<()> {
    let socket_path = config.socket_path.clone();
    let envelope = config.into_envelope();
    envelope.validate()?;

    // The sender is intentionally thin: it just opens the daemon socket and writes one frame.
    let mut stream = UnixStream::connect(&socket_path).await?;
    write_envelope(&mut stream, &envelope).await
}

fn next_value<I>(args: &mut I, flag: &str) -> Result<String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| BusError::Configuration(format!("missing value after `{flag}`")))
}

fn require_value<T>(value: Option<T>, flag: &str) -> Result<T> {
    value.ok_or_else(|| BusError::Configuration(format!("missing required argument `{flag}`")))
}

fn parse_header_arg(value: &str) -> Result<(String, String)> {
    let (key, header_value) = value.split_once('=').ok_or_else(|| {
        BusError::Configuration(format!("invalid header `{value}`: expected `key=value`"))
    })?;

    if key.trim().is_empty() {
        return Err(BusError::Configuration(format!(
            "invalid header `{value}`: key must not be empty"
        )));
    }

    Ok((key.to_string(), header_value.to_string()))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use tlbus_core::read_envelope;
    use tokio::net::UnixListener;

    use super::{SendConfig, send};

    #[test]
    fn parses_send_arguments() {
        let config = SendConfig::parse_from([
            "--socket",
            "/tmp/tlb.sock",
            "--from",
            "client",
            "--to",
            "echo",
            "--ttl-ms",
            "2500",
            "--header",
            "trace_id=abc123",
            "--protocol",
            "mcp",
            "--transport",
            "http2",
            "--protocol-version",
            "2025-06-18",
            "--content-type",
            "application/json",
            "--payload",
            "hello",
        ])
        .unwrap();

        assert_eq!(
            config.socket_path,
            std::path::PathBuf::from("/tmp/tlb.sock")
        );
        assert_eq!(config.from, "client");
        assert_eq!(config.to, "echo");
        assert_eq!(config.ttl_ms, 2_500);
        assert_eq!(config.headers.get("trace_id"), Some(&"abc123".to_string()));
        assert_eq!(config.headers.get("protocol"), Some(&"mcp".to_string()));
        assert_eq!(config.headers.get("transport"), Some(&"http2".to_string()));
        assert_eq!(
            config.headers.get("protocol_version"),
            Some(&"2025-06-18".to_string())
        );
        assert_eq!(
            config.headers.get("content_type"),
            Some(&"application/json".to_string())
        );
        assert_eq!(config.payload, b"hello".to_vec());
    }

    #[test]
    fn rejects_invalid_header_format() {
        let error = SendConfig::parse_from([
            "--from",
            "client",
            "--to",
            "echo",
            "--header",
            "broken",
            "--payload",
            "hello",
        ])
        .unwrap_err();

        assert!(error.to_string().contains("expected `key=value`"));
    }

    #[tokio::test]
    async fn sends_one_envelope_to_the_daemon_socket() {
        let tempdir = tempdir().unwrap();
        let socket_path = tempdir.path().join("tlb.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();

        let sender = tokio::spawn({
            let socket_path = socket_path.clone();
            async move {
                send(
                    SendConfig::parse_from([
                        "--socket",
                        socket_path.to_string_lossy().as_ref(),
                        "--from",
                        "client",
                        "--to",
                        "echo",
                        "--header",
                        "trace_id=abc123",
                        "--payload",
                        "hello world",
                    ])
                    .unwrap(),
                )
                .await
                .unwrap()
            }
        });

        let (mut inbound, _) = listener.accept().await.unwrap();
        let envelope = read_envelope(&mut inbound).await.unwrap();
        sender.await.unwrap();

        assert_eq!(envelope.from, "client");
        assert_eq!(envelope.to, "echo");
        assert_eq!(envelope.payload, b"hello world".to_vec());
        assert_eq!(
            envelope.headers.get("trace_id"),
            Some(&"abc123".to_string())
        );
    }
}
