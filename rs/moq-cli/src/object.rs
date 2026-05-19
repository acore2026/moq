use anyhow::Context;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAGIC: &[u8; 4] = b"MOBJ";
pub const VERSION: u8 = 1;
pub const OP_PUBLISH: u8 = 1;
pub const OP_UNPUBLISH: u8 = 2;
pub const OP_OBJECT: u8 = 3;
pub const HEADER_LEN: usize = 4 + 1 + 1 + 2 + 8 + 8 + 4;
pub const WIRE_MAGIC: &[u8; 4] = b"MOBW";
pub const WIRE_VERSION: u8 = 1;
pub const WIRE_HEADER_LEN: usize = 4 + 1 + 8 + 8 + 4;

#[derive(Debug)]
pub struct ObjectFrame {
	pub op: u8,
	pub track: String,
	pub group_id: u64,
	pub object_id: u64,
	pub payload: bytes::Bytes,
}

pub async fn read_frame<R>(reader: &mut R) -> anyhow::Result<Option<ObjectFrame>>
where
	R: AsyncRead + Unpin,
{
	let mut header = [0u8; HEADER_LEN];
	match reader.read_exact(&mut header).await {
		Ok(_) => {}
		Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
		Err(err) => return Err(err).context("failed to read object frame header"),
	}

	if &header[0..4] != MAGIC {
		anyhow::bail!("invalid object frame magic");
	}
	if header[4] != VERSION {
		anyhow::bail!("unsupported object frame version: {}", header[4]);
	}

	let op = header[5];
	let track_len = u16::from_be_bytes(header[6..8].try_into()?) as usize;
	let group_id = u64::from_be_bytes(header[8..16].try_into()?);
	let object_id = u64::from_be_bytes(header[16..24].try_into()?);
	let payload_len = u32::from_be_bytes(header[24..28].try_into()?) as usize;

	let mut track = vec![0u8; track_len];
	reader
		.read_exact(&mut track)
		.await
		.context("failed to read object frame track")?;
	let track = String::from_utf8(track).context("object frame track is not valid UTF-8")?;

	let mut payload = vec![0u8; payload_len];
	reader
		.read_exact(&mut payload)
		.await
		.context("failed to read object frame payload")?;

	Ok(Some(ObjectFrame {
		op,
		track,
		group_id,
		object_id,
		payload: payload.into(),
	}))
}

pub async fn write_frame<W>(writer: &mut W, frame: &ObjectFrame) -> anyhow::Result<()>
where
	W: AsyncWrite + Unpin,
{
	let track = frame.track.as_bytes();
	if track.len() > u16::MAX as usize {
		anyhow::bail!("object frame track is too long");
	}
	if frame.payload.len() > u32::MAX as usize {
		anyhow::bail!("object frame payload is too large");
	}

	writer.write_all(MAGIC).await?;
	writer.write_all(&[VERSION, frame.op]).await?;
	writer.write_all(&(track.len() as u16).to_be_bytes()).await?;
	writer.write_all(&frame.group_id.to_be_bytes()).await?;
	writer.write_all(&frame.object_id.to_be_bytes()).await?;
	writer.write_all(&(frame.payload.len() as u32).to_be_bytes()).await?;
	writer.write_all(track).await?;
	writer.write_all(&frame.payload).await?;
	writer.flush().await?;
	Ok(())
}

pub fn encode_wire_payload(frame: &ObjectFrame) -> anyhow::Result<bytes::Bytes> {
	if frame.payload.len() > u32::MAX as usize {
		anyhow::bail!("object frame payload is too large");
	}

	let mut payload = Vec::with_capacity(WIRE_HEADER_LEN + frame.payload.len());
	payload.extend_from_slice(WIRE_MAGIC);
	payload.push(WIRE_VERSION);
	payload.extend_from_slice(&frame.group_id.to_be_bytes());
	payload.extend_from_slice(&frame.object_id.to_be_bytes());
	payload.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
	payload.extend_from_slice(&frame.payload);
	Ok(payload.into())
}

pub fn decode_wire_payload(fallback_sequence: u64, payload: bytes::Bytes) -> anyhow::Result<(u64, u64, bytes::Bytes)> {
	if payload.len() < WIRE_HEADER_LEN || &payload[0..4] != WIRE_MAGIC {
		return Ok((fallback_sequence, fallback_sequence, payload));
	}
	if payload[4] != WIRE_VERSION {
		anyhow::bail!("unsupported object wire payload version: {}", payload[4]);
	}

	let group_id = u64::from_be_bytes(payload[5..13].try_into()?);
	let object_id = u64::from_be_bytes(payload[13..21].try_into()?);
	let payload_len = u32::from_be_bytes(payload[21..25].try_into()?) as usize;
	if payload.len() != WIRE_HEADER_LEN + payload_len {
		anyhow::bail!("invalid object wire payload length");
	}

	Ok((group_id, object_id, payload.slice(WIRE_HEADER_LEN..)))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn wire_payload_round_trips_object_location() {
		let frame = ObjectFrame {
			op: OP_OBJECT,
			track: "Location".to_string(),
			group_id: 2,
			object_id: 7,
			payload: bytes::Bytes::from_static(b"payload"),
		};

		let payload = encode_wire_payload(&frame).unwrap();
		let decoded = decode_wire_payload(99, payload).unwrap();

		assert_eq!(decoded.0, 2);
		assert_eq!(decoded.1, 7);
		assert_eq!(&decoded.2[..], b"payload");
	}

	#[test]
	fn raw_payload_falls_back_to_group_sequence() {
		let decoded = decode_wire_payload(9, bytes::Bytes::from_static(b"legacy")).unwrap();

		assert_eq!(decoded.0, 9);
		assert_eq!(decoded.1, 9);
		assert_eq!(&decoded.2[..], b"legacy");
	}
}
