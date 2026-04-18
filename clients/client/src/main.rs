use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use serde_json::{Value, json};
use tlbus_client::{DEFAULT_BUS_SOCKET, EndpointConfig, parse_header_pairs};
use tlbus_core::{BusError, REPLY_TO_HEADER, Result, TXN_ID_HEADER};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct RegisterArgs {
    bus_socket: PathBuf,
    service_name: String,
    service_secret: String,
}

#[derive(Debug, Clone)]
struct SendArgs {
    bus_socket: PathBuf,
    service_name: String,
    service_secret: String,
    target: String,
    payload: String,
    headers: Vec<String>,
}

#[derive(Debug, Clone)]
struct RecvArgs {
    bus_socket: PathBuf,
    service_name: String,
    service_secret: String,
    timeout_seconds: u64,
}

#[derive(Debug, Clone)]
enum Command {
    Register(RegisterArgs),
    Send(SendArgs),
    Recv(RecvArgs),
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(BusError::Configuration(message)) if message == "help requested" => {
            print_usage();
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("tlbus-client: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<()> {
    match parse_command(env::args().skip(1))? {
        Command::Register(args) => run_register(args),
        Command::Send(args) => run_send(args),
        Command::Recv(args) => run_recv(args),
    }
}

fn parse_command<I>(mut args: I) -> Result<Command>
where
    I: Iterator<Item = String>,
{
    let Some(command) = args.next() else {
        return Err(BusError::Configuration("help requested".to_string()));
    };

    match command.as_str() {
        "register" => parse_register(args).map(Command::Register),
        "send" => parse_send(args).map(Command::Send),
        "recv" => parse_recv(args).map(Command::Recv),
        "-h" | "--help" => Err(BusError::Configuration("help requested".to_string())),
        other => Err(BusError::Configuration(format!(
            "unknown command `{other}`"
        ))),
    }
}

fn parse_register<I>(mut args: I) -> Result<RegisterArgs>
where
    I: Iterator<Item = String>,
{
    let mut bus_socket = PathBuf::from(DEFAULT_BUS_SOCKET);
    let mut service_name = None;
    let mut service_secret = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bus-socket" => bus_socket = PathBuf::from(next_value(&mut args, "--bus-socket")?),
            "--service-name" => service_name = Some(next_value(&mut args, "--service-name")?),
            "--service-secret" => service_secret = Some(next_value(&mut args, "--service-secret")?),
            "-h" | "--help" => return Err(BusError::Configuration("help requested".to_string())),
            _ => return Err(BusError::Configuration(format!("unknown argument `{arg}`"))),
        }
    }

    Ok(RegisterArgs {
        bus_socket,
        service_name: require_value(service_name, "--service-name")?,
        service_secret: require_value(service_secret, "--service-secret")?,
    })
}

fn parse_send<I>(mut args: I) -> Result<SendArgs>
where
    I: Iterator<Item = String>,
{
    let mut bus_socket = PathBuf::from(DEFAULT_BUS_SOCKET);
    let mut service_name = None;
    let mut service_secret = None;
    let mut target = None;
    let mut payload = None;
    let mut headers = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bus-socket" => bus_socket = PathBuf::from(next_value(&mut args, "--bus-socket")?),
            "--service-name" => service_name = Some(next_value(&mut args, "--service-name")?),
            "--service-secret" => service_secret = Some(next_value(&mut args, "--service-secret")?),
            "--to" => target = Some(next_value(&mut args, "--to")?),
            "--payload" => payload = Some(next_value(&mut args, "--payload")?),
            "--header" => headers.push(next_value(&mut args, "--header")?),
            "-h" | "--help" => return Err(BusError::Configuration("help requested".to_string())),
            _ => return Err(BusError::Configuration(format!("unknown argument `{arg}`"))),
        }
    }

    Ok(SendArgs {
        bus_socket,
        service_name: require_value(service_name, "--service-name")?,
        service_secret: require_value(service_secret, "--service-secret")?,
        target: require_value(target, "--to")?,
        payload: require_value(payload, "--payload")?,
        headers,
    })
}

fn parse_recv<I>(mut args: I) -> Result<RecvArgs>
where
    I: Iterator<Item = String>,
{
    let mut bus_socket = PathBuf::from(DEFAULT_BUS_SOCKET);
    let mut service_name = None;
    let mut service_secret = None;
    let mut timeout_seconds = 20_u64;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bus-socket" => bus_socket = PathBuf::from(next_value(&mut args, "--bus-socket")?),
            "--service-name" => service_name = Some(next_value(&mut args, "--service-name")?),
            "--service-secret" => service_secret = Some(next_value(&mut args, "--service-secret")?),
            "--timeout-seconds" => {
                let raw = next_value(&mut args, "--timeout-seconds")?;
                timeout_seconds = raw.parse().map_err(|_| {
                    BusError::Configuration(format!(
                        "invalid `--timeout-seconds` value `{raw}`: expected integer"
                    ))
                })?;
            }
            "-h" | "--help" => return Err(BusError::Configuration("help requested".to_string())),
            _ => return Err(BusError::Configuration(format!("unknown argument `{arg}`"))),
        }
    }

    Ok(RecvArgs {
        bus_socket,
        service_name: require_value(service_name, "--service-name")?,
        service_secret: require_value(service_secret, "--service-secret")?,
        timeout_seconds,
    })
}

fn run_register(args: RegisterArgs) -> Result<()> {
    let endpoint = EndpointConfig::new(args.service_name, args.service_secret)
        .with_bus_socket(args.bus_socket)
        .register()?;

    let output = json!({
        "service": endpoint.service_name(),
        "service_socket": endpoint.service_socket,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&output)
            .map_err(|error| BusError::Configuration(error.to_string()))?
    );
    eprintln!(
        "tlbus-client: event=register service={} service_socket={}",
        endpoint.service_name(),
        endpoint.service_socket.display()
    );
    Ok(())
}

fn run_send(args: SendArgs) -> Result<()> {
    let endpoint = EndpointConfig::new(args.service_name, args.service_secret)
        .with_bus_socket(args.bus_socket)
        .register()?;

    let payload = parse_json_or_text(&args.payload);
    let mut headers = parse_header_pairs(&args.headers)?;
    let txn_id = headers
        .entry(TXN_ID_HEADER.to_string())
        .or_insert_with(|| Uuid::new_v4().to_string())
        .to_string();
    let reply_to = headers
        .entry(REPLY_TO_HEADER.to_string())
        .or_insert_with(|| format!("{}.inbox", endpoint.service_name()))
        .to_string();

    endpoint.send_json(&args.target, &payload, headers)?;
    eprintln!(
        "tlbus-client: event=send from={} to={} txn_id={} reply_to={}",
        endpoint.service_name(),
        args.target,
        txn_id,
        reply_to
    );
    Ok(())
}

fn run_recv(args: RecvArgs) -> Result<()> {
    let endpoint = EndpointConfig::new(args.service_name, args.service_secret)
        .with_bus_socket(args.bus_socket)
        .register()?;

    let envelope = endpoint.receive_once(Duration::from_secs(args.timeout_seconds))?;
    let trace_fields = envelope.trace_fields();
    let payload = parse_bytes_payload(&envelope.payload);
    let output = json!({
        "from": envelope.from,
        "to": envelope.to,
        "headers": envelope.headers,
        "payload": payload,
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&output)
            .map_err(|error| BusError::Configuration(error.to_string()))?
    );
    eprintln!(
        "tlbus-client: event=recv service={} {}",
        endpoint.service_name(),
        trace_fields
    );
    Ok(())
}

fn parse_json_or_text(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

fn parse_bytes_payload(payload: &[u8]) -> Value {
    serde_json::from_slice(payload)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(payload).into_owned()))
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

fn print_usage() {
    eprintln!("usage:");
    eprintln!(
        "  tlbus-client register --service-name <name> --service-secret <secret> [--bus-socket /run/tlb.sock]"
    );
    eprintln!(
        "  tlbus-client send --service-name <name> --service-secret <secret> --to <target> --payload <json-or-text> [--header key=value ...]"
    );
    eprintln!(
        "  tlbus-client recv --service-name <name> --service-secret <secret> [--timeout-seconds 20]"
    );
    eprintln!();
    eprintln!("subcommands:");
    eprintln!("  register   Registers the client and prints the resolved service socket");
    eprintln!("  send       Registers the client and sends one envelope");
    eprintln!("  recv       Registers the client and waits for one inbound envelope");
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{parse_bytes_payload, parse_json_or_text};

    #[test]
    fn parse_json_or_text_accepts_json() {
        let parsed = parse_json_or_text("{\"ok\":true}");
        assert_eq!(parsed["ok"], Value::Bool(true));
    }

    #[test]
    fn parse_json_or_text_falls_back_to_string() {
        let parsed = parse_json_or_text("plain-text");
        assert_eq!(parsed, Value::String("plain-text".to_string()));
    }

    #[test]
    fn parse_bytes_payload_falls_back_to_utf8() {
        let parsed = parse_bytes_payload(b"hello");
        assert_eq!(parsed, Value::String("hello".to_string()));
    }
}
