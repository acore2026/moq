use clap::ValueEnum;

use crate::object;
use hang::moq_lite;
use tokio::io::AsyncWriteExt;

const AVC3_EPOCH_MS_SEI_UUID: &[u8; 16] = b"moq-avc3-ts-ms!!";

#[derive(ValueEnum, Clone, Copy)]
pub enum OutputFormat {
	Avc3,
	Fmp4,
	Object,
}

#[derive(clap::Args, Clone)]
pub struct SubscribeArgs {
	/// Output format for stdout
	#[arg(long)]
	pub output: OutputFormat,

	/// Maximum latency in milliseconds before skipping groups
	#[arg(long, default_value = "500")]
	pub max_latency: u64,

	/// Track to subscribe when output=object
	#[arg(long)]
	pub track: Option<String>,

	/// First group to request when output=object
	#[arg(long)]
	pub start_group: Option<u64>,

	/// First object to emit when output=object
	#[arg(long, default_value = "0")]
	pub start_object: u64,

	/// Last group to emit when output=object
	#[arg(long)]
	pub end_group: Option<u64>,

	/// Last object to emit when output=object
	#[arg(long)]
	pub end_object: Option<u64>,
}

pub struct Subscribe {
	broadcast: moq_lite::BroadcastConsumer,
	args: SubscribeArgs,
}

impl Subscribe {
	pub fn new(broadcast: moq_lite::BroadcastConsumer, args: SubscribeArgs) -> Self {
		Self { broadcast, args }
	}

	pub async fn run(self) -> anyhow::Result<()> {
		match self.args.output {
			OutputFormat::Avc3 => self.run_avc3().await,
			OutputFormat::Fmp4 => self.run_fmp4().await,
			OutputFormat::Object => self.run_object().await,
		}
	}

	async fn run_object(self) -> anyhow::Result<()> {
		let track_name = self
			.args
			.track
			.clone()
			.ok_or_else(|| anyhow::anyhow!("--track is required when --output object"))?;
		let mut stdout = tokio::io::stdout();
		let request =
			moq_lite::Track::new(track_name.clone()).with_group_range(self.args.start_group, self.args.end_group);
		let mut track = self.broadcast.subscribe_track(&request)?;

		while let Some(mut group) = track.recv_group().await? {
			while let Some(payload) = group.read_frame().await? {
				let (group_id, object_id, payload) = object::decode_wire_payload(group.sequence, payload)?;
				if self.args.start_group.is_some_and(|start| group_id < start) {
					continue;
				}
				if object_id < self.args.start_object {
					continue;
				}
				if self.args.end_group.is_some_and(|end| group_id > end) {
					continue;
				}
				if self.args.end_object.is_some_and(|end| object_id > end) {
					continue;
				}

				let frame = object::ObjectFrame {
					op: object::OP_OBJECT,
					track: track_name.clone(),
					group_id,
					object_id,
					payload,
				};
				object::write_frame(&mut stdout, &frame).await?;
			}
		}

		Ok(())
	}

	async fn run_avc3(self) -> anyhow::Result<()> {
		let mut stdout = tokio::io::stdout();
		let max_latency = std::time::Duration::from_millis(self.args.max_latency);

		let catalog_track = self.broadcast.subscribe_track(&hang::Catalog::default_track())?;
		let mut catalog = moq_mux::catalog::Consumer::new(catalog_track);

		let (track_name, container) = loop {
			let snapshot = catalog
				.next()
				.await?
				.ok_or_else(|| anyhow::anyhow!("broadcast ended before catalog was announced"))?;
			if let Some((name, config)) = snapshot.video.renditions.into_iter().next() {
				break (name, config.container);
			}
		};

		let media: moq_mux::container::Hang = (&container).try_into()?;
		let track = self.broadcast.subscribe_track(&moq_lite::Track::new(track_name))?;
		let mut consumer = moq_mux::container::Consumer::new(track, media).with_latency(max_latency);

		while let Some(frame) = consumer.read().await? {
			let sent_epoch_ms = extract_avc3_epoch_ms_sei(&frame.payload).unwrap_or(0);
			stdout.write_all(b"MAVT").await?;
			let timestamp_us = frame.timestamp.as_micros().min(u64::MAX as u128) as u64;
			stdout.write_all(&timestamp_us.to_be_bytes()).await?;
			stdout.write_all(&[u8::from(frame.keyframe)]).await?;
			stdout.write_all(&sent_epoch_ms.to_be_bytes()).await?;
			stdout.write_all(&(frame.payload.len() as u32).to_be_bytes()).await?;
			stdout.write_all(&frame.payload).await?;
			stdout.flush().await?;
		}

		Ok(())
	}

	async fn run_fmp4(self) -> anyhow::Result<()> {
		let mut stdout = tokio::io::stdout();
		let max_latency = std::time::Duration::from_millis(self.args.max_latency);

		// Fmp4 subscribes to the catalog internally, builds the merged init segment
		// from the first catalog snapshot, then yields moof+mdat fragments in
		// timestamp order across tracks.
		let mut fmp4 = moq_mux::export::Fmp4::new(self.broadcast)?.with_latency(max_latency);

		while let Some(chunk) = fmp4.next().await? {
			stdout.write_all(&chunk).await?;
			stdout.flush().await?;
		}

		Ok(())
	}
}

fn extract_avc3_epoch_ms_sei(payload: &[u8]) -> Option<u64> {
	let mut offset = 0;
	while let Some((nal_start, nal_end, next_offset)) = next_annexb_nal(payload, offset) {
		offset = next_offset;
		if nal_start >= nal_end || payload[nal_start] & 0x1f != 6 {
			continue;
		}
		if let Some(sent_epoch_ms) = parse_epoch_ms_sei(&payload[nal_start + 1..nal_end]) {
			return Some(sent_epoch_ms);
		}
	}
	None
}

fn next_annexb_nal(payload: &[u8], offset: usize) -> Option<(usize, usize, usize)> {
	let prefix = find_start_code(payload, offset)?;
	let nal_start = if payload.get(prefix + 2) == Some(&1) {
		prefix + 3
	} else {
		prefix + 4
	};
	let next_prefix = find_start_code(payload, nal_start).unwrap_or(payload.len());
	Some((nal_start, next_prefix, next_prefix))
}

fn find_start_code(payload: &[u8], offset: usize) -> Option<usize> {
	let mut i = offset;
	while i + 3 <= payload.len() {
		if payload[i] == 0 && payload[i + 1] == 0 {
			if payload[i + 2] == 1 {
				return Some(i);
			}
			if i + 4 <= payload.len() && payload[i + 2] == 0 && payload[i + 3] == 1 {
				return Some(i);
			}
		}
		i += 1;
	}
	None
}

fn parse_epoch_ms_sei(ebsp: &[u8]) -> Option<u64> {
	let rbsp = remove_emulation_prevention(ebsp);
	let mut offset = 0;

	while offset < rbsp.len() {
		if rbsp[offset] == 0x80 {
			break;
		}

		let payload_type = read_sei_value(&rbsp, &mut offset)?;
		let payload_size = read_sei_value(&rbsp, &mut offset)? as usize;
		if offset + payload_size > rbsp.len() {
			return None;
		}

		let sei_payload = &rbsp[offset..offset + payload_size];
		if payload_type == 5 && sei_payload.len() >= 24 && &sei_payload[..16] == AVC3_EPOCH_MS_SEI_UUID {
			return Some(u64::from_be_bytes(sei_payload[16..24].try_into().ok()?));
		}

		offset += payload_size;
	}

	None
}

fn read_sei_value(rbsp: &[u8], offset: &mut usize) -> Option<u64> {
	let mut value = 0u64;
	while *offset < rbsp.len() {
		let byte = rbsp[*offset];
		*offset += 1;
		value += u64::from(byte);
		if byte != 0xff {
			return Some(value);
		}
	}
	None
}

fn remove_emulation_prevention(ebsp: &[u8]) -> Vec<u8> {
	let mut rbsp = Vec::with_capacity(ebsp.len());
	let mut i = 0;
	while i < ebsp.len() {
		if i + 2 < ebsp.len() && ebsp[i] == 0 && ebsp[i + 1] == 0 && ebsp[i + 2] == 3 {
			rbsp.extend_from_slice(&[0, 0]);
			i += 3;
		} else {
			rbsp.push(ebsp[i]);
			i += 1;
		}
	}
	rbsp
}
