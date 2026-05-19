use clap::ValueEnum;

use crate::object;
use hang::moq_lite;
use tokio::io::AsyncWriteExt;

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
			stdout.write_all(b"MAVC").await?;
			let timestamp_us = frame.timestamp.as_micros().min(u64::MAX as u128) as u64;
			stdout.write_all(&timestamp_us.to_be_bytes()).await?;
			stdout.write_all(&[u8::from(frame.keyframe)]).await?;
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
