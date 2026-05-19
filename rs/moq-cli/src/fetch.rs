use clap::ValueEnum;

use crate::object;
use hang::moq_lite;
use tokio::io::AsyncWriteExt;

#[derive(ValueEnum, Clone, Copy)]
pub enum FetchOutputFormat {
	Object,
}

#[derive(clap::Args, Clone)]
pub struct FetchArgs {
	/// Output format for stdout
	#[arg(long)]
	pub output: FetchOutputFormat,

	/// Track to fetch
	#[arg(long)]
	pub track: String,

	/// First group to request
	#[arg(long, default_value = "0")]
	pub start_group: u64,

	/// First object to emit
	#[arg(long, default_value = "0")]
	pub start_object: u64,

	/// Last group to emit. If omitted, fetch exits after the idle timeout.
	#[arg(long)]
	pub end_group: Option<u64>,

	/// Last object to emit
	#[arg(long)]
	pub end_object: Option<u64>,

	/// Exit after this many milliseconds without receiving a group.
	#[arg(long, default_value = "250")]
	pub idle_timeout_ms: u64,
}

pub struct Fetch {
	broadcast: moq_lite::BroadcastConsumer,
	args: FetchArgs,
}

impl Fetch {
	pub fn new(broadcast: moq_lite::BroadcastConsumer, args: FetchArgs) -> Self {
		Self { broadcast, args }
	}

	pub async fn run(self) -> anyhow::Result<()> {
		match self.args.output {
			FetchOutputFormat::Object => self.run_object().await,
		}
	}

	async fn run_object(self) -> anyhow::Result<()> {
		let mut stdout = tokio::io::stdout();
		let request = moq_lite::Track::new(self.args.track.clone())
			.with_group_range(Some(self.args.start_group), self.args.end_group);
		let mut track = self.broadcast.subscribe_track(&request)?;
		let idle_timeout = std::time::Duration::from_millis(self.args.idle_timeout_ms);

		loop {
			let group = tokio::time::timeout(idle_timeout, track.recv_group()).await;
			let Ok(group) = group else {
				break;
			};
			let Some(mut group) = group? else {
				break;
			};

			while let Some(payload) = group.read_frame().await? {
				let (group_id, object_id, payload) = object::decode_wire_payload(group.sequence, payload)?;
				if group_id < self.args.start_group {
					continue;
				}
				if object_id < self.args.start_object {
					continue;
				}
				if self.args.end_group.is_some_and(|end| group_id > end) {
					break;
				}
				if self.args.end_object.is_some_and(|end| object_id > end) {
					continue;
				}

				let frame = object::ObjectFrame {
					op: object::OP_OBJECT,
					track: self.args.track.clone(),
					group_id,
					object_id,
					payload,
				};
				object::write_frame(&mut stdout, &frame).await?;
			}

			if self.args.end_group.is_some_and(|end| group.sequence >= end) {
				break;
			}
		}

		stdout.flush().await?;
		Ok(())
	}
}
