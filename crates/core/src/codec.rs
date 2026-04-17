use std::io::{Read, Write};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{BusError, BusFrame, Envelope, Result};

const MAX_FRAME_LEN: usize = 8 * 1024 * 1024;

pub fn encode_frame(frame: &BusFrame) -> Result<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(frame)?)
}

pub fn decode_frame(bytes: &[u8]) -> Result<BusFrame> {
    Ok(rmp_serde::from_slice(bytes)?)
}

pub fn encode_envelope(envelope: &Envelope) -> Result<Vec<u8>> {
    encode_frame(&BusFrame::Envelope(envelope.clone()))
}

pub fn decode_envelope(bytes: &[u8]) -> Result<Envelope> {
    decode_frame(bytes)?.into_envelope()
}

pub fn write_frame_sync<W>(writer: &mut W, frame: &BusFrame) -> Result<()>
where
    W: Write,
{
    let payload = encode_frame(frame)?;
    let frame_len = u32::try_from(payload.len())
        .map_err(|_| BusError::Codec("frame exceeds 4GiB frame limit".to_string()))?;

    writer.write_all(&frame_len.to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()?;

    Ok(())
}

pub fn read_frame_sync<R>(reader: &mut R) -> Result<BusFrame>
where
    R: Read,
{
    let mut len_buf = [0_u8; 4];
    reader.read_exact(&mut len_buf)?;

    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len > MAX_FRAME_LEN {
        return Err(BusError::Codec(format!(
            "incoming frame size {frame_len} exceeds the {MAX_FRAME_LEN} byte limit"
        )));
    }

    let mut payload = vec![0_u8; frame_len];
    reader.read_exact(&mut payload)?;
    decode_frame(&payload)
}

pub fn write_envelope_sync<W>(writer: &mut W, envelope: &Envelope) -> Result<()>
where
    W: Write,
{
    write_frame_sync(writer, &BusFrame::Envelope(envelope.clone()))
}

pub fn read_envelope_sync<R>(reader: &mut R) -> Result<Envelope>
where
    R: Read,
{
    read_frame_sync(reader)?.into_envelope()
}

pub async fn write_frame<W>(writer: &mut W, frame: &BusFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = encode_frame(frame)?;
    let frame_len = u32::try_from(payload.len())
        .map_err(|_| BusError::Codec("frame exceeds 4GiB frame limit".to_string()))?;

    writer.write_u32_le(frame_len).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;

    Ok(())
}

pub async fn read_frame<R>(reader: &mut R) -> Result<BusFrame>
where
    R: AsyncRead + Unpin,
{
    let frame_len = reader.read_u32_le().await? as usize;
    if frame_len > MAX_FRAME_LEN {
        return Err(BusError::Codec(format!(
            "incoming frame size {frame_len} exceeds the {MAX_FRAME_LEN} byte limit"
        )));
    }

    let mut payload = vec![0_u8; frame_len];
    reader.read_exact(&mut payload).await?;
    decode_frame(&payload)
}

pub async fn write_envelope<W>(writer: &mut W, envelope: &Envelope) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    // v0.1 keeps framing intentionally simple: a little-endian size prefix
    // followed by one msgpack-encoded frame carrying either a message or control data.
    write_frame(writer, &BusFrame::Envelope(envelope.clone())).await
}

pub async fn read_envelope<R>(reader: &mut R) -> Result<Envelope>
where
    R: AsyncRead + Unpin,
{
    read_frame(reader).await?.into_envelope()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use crate::{
        BusFrame, Envelope, ServiceCapability, ServiceManifest, ServiceMode,
        ServiceRegistrationRequest, decode_envelope, decode_frame, encode_envelope, encode_frame,
        read_envelope_sync, read_frame_sync, write_envelope_sync, write_frame_sync,
    };

    #[test]
    fn round_trips_through_msgpack() {
        let mut envelope = Envelope::new("svc-a", "svc-b", b"hello".to_vec());
        envelope
            .headers
            .insert("txn_id".to_string(), "123".to_string());

        let encoded = encode_envelope(&envelope).unwrap();
        let decoded = decode_envelope(&encoded).unwrap();

        assert_eq!(decoded, envelope);
    }

    #[test]
    fn round_trips_registration_frame_through_msgpack() {
        let frame = BusFrame::ServiceRegistrationRequest(ServiceRegistrationRequest {
            manifest: ServiceManifest {
                name: "ps2.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: vec![ServiceCapability {
                    name: "calculate".to_string(),
                    address: "ps2.calcola.compute".to_string(),
                    description: "Evaluates an arithmetic expression".to_string(),
                }],
                modes: vec![ServiceMode {
                    transport: "http2".to_string(),
                    protocol: "mcp".to_string(),
                    protocol_version: Some("2025-06-18".to_string()),
                    content_type: Some("application/json".to_string()),
                }],
            },
        });

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded, frame);
    }

    #[test]
    fn sync_helpers_round_trip_envelope_with_length_prefix() {
        let envelope = Envelope::new("svc-a", "svc-b", b"hello".to_vec());
        let mut bytes = Vec::new();

        write_envelope_sync(&mut bytes, &envelope).unwrap();
        let decoded = read_envelope_sync(&mut Cursor::new(bytes)).unwrap();

        assert_eq!(decoded, envelope);
    }

    #[test]
    fn sync_helpers_round_trip_generic_frame_with_length_prefix() {
        let frame = BusFrame::ServiceRegistrationRequest(ServiceRegistrationRequest {
            manifest: ServiceManifest {
                name: "ps2.calcola".to_string(),
                secret: "shared-secret".to_string(),
                is_client: false,
                features: BTreeMap::new(),
                capabilities: vec![ServiceCapability {
                    name: "calculate".to_string(),
                    address: "ps2.calcola.compute".to_string(),
                    description: "Evaluates an arithmetic expression".to_string(),
                }],
                modes: vec![ServiceMode {
                    transport: "http2".to_string(),
                    protocol: "mcp".to_string(),
                    protocol_version: Some("2025-06-18".to_string()),
                    content_type: Some("application/json".to_string()),
                }],
            },
        });
        let mut bytes = Vec::new();

        write_frame_sync(&mut bytes, &frame).unwrap();
        let decoded = read_frame_sync(&mut Cursor::new(bytes)).unwrap();

        assert_eq!(decoded, frame);
    }
}
