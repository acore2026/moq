use clap::Subcommand;
use std::collections::HashMap;

use crate::object;
use hang::moq_lite;
use moq_mux::import;

#[derive(Subcommand, Clone)]
pub enum PublishFormat {
	Avc3,
	Fmp4,
	/// Publish arbitrary object frames from stdin.
	Object,
	// NOTE: No aac support because it needs framing.
	Hls {
		/// URL or file path of an HLS playlist to ingest.
		#[arg(long)]
		playlist: String,
	},
}

enum PublishDecoder {
	Avc3(Box<import::Avc3>),
	Fmp4(Box<import::Fmp4>),
	Object(ObjectPublisher),
	Hls(Box<import::Hls>),
}

impl PublishDecoder {
	/// Decode a chunk of bytes from stdin (Avc3 or Fmp4 only).
	fn decode_buf(&mut self, buffer: &mut bytes::BytesMut) -> anyhow::Result<()> {
		match self {
			Self::Avc3(d) => d.decode_stream(buffer, None),
			Self::Fmp4(d) => d.decode(buffer),
			Self::Object(_) => unreachable!(),
			Self::Hls(_) => unreachable!(),
		}
	}
}

struct ObjectPublisher {
	broadcast: moq_lite::BroadcastProducer,
	tracks: HashMap<String, moq_lite::TrackProducer>,
}

impl ObjectPublisher {
	fn new(broadcast: moq_lite::BroadcastProducer) -> Self {
		Self {
			broadcast,
			tracks: HashMap::new(),
		}
	}

	fn publish_track(&mut self, name: &str) -> anyhow::Result<()> {
		if self.tracks.contains_key(name) {
			return Ok(());
		}

		let track = self.broadcast.create_track(moq_lite::Track::new(name))?;
		self.tracks.insert(name.to_string(), track);
		Ok(())
	}

	fn unpublish_track(&mut self, name: &str) -> anyhow::Result<()> {
		self.tracks.remove(name);
		match self.broadcast.remove_track(name) {
			Ok(()) | Err(moq_lite::Error::NotFound) => Ok(()),
			Err(err) => Err(err.into()),
		}
	}

	fn write_object(&mut self, frame: object::ObjectFrame) -> anyhow::Result<()> {
		self.publish_track(&frame.track)?;
		let track = self
			.tracks
			.get_mut(&frame.track)
			.ok_or_else(|| anyhow::anyhow!("track was not published: {}", frame.track))?;

		let mut group = track.create_group(moq_lite::Group {
			sequence: frame.object_id,
		})?;
		group.write_frame(object::encode_wire_payload(&frame)?)?;
		group.finish()?;
		Ok(())
	}

	async fn run(mut self) -> anyhow::Result<()> {
		let mut stdin = tokio::io::stdin();

		while let Some(frame) = object::read_frame(&mut stdin).await? {
			match frame.op {
				object::OP_PUBLISH => self.publish_track(&frame.track)?,
				object::OP_UNPUBLISH => self.unpublish_track(&frame.track)?,
				object::OP_OBJECT => self.write_object(frame)?,
				other => anyhow::bail!("unsupported object frame op: {other}"),
			}
		}

		for (_, mut track) in self.tracks {
			let _ = track.finish();
		}

		Ok(())
	}
}

pub struct Publish {
	decoder: PublishDecoder,
	broadcast: moq_lite::BroadcastProducer,
}

impl Publish {
	pub fn new(format: &PublishFormat) -> anyhow::Result<Self> {
		let mut broadcast = moq_lite::Broadcast::new().produce();
		let catalog = moq_mux::catalog::Producer::new(&mut broadcast)?;

		let decoder = match format {
			PublishFormat::Avc3 => {
				let avc3 = import::Avc3::new(broadcast.clone(), catalog.clone());
				PublishDecoder::Avc3(Box::new(avc3))
			}
			PublishFormat::Fmp4 => {
				let fmp4 = import::Fmp4::new(broadcast.clone(), catalog.clone());
				PublishDecoder::Fmp4(Box::new(fmp4))
			}
			PublishFormat::Object => PublishDecoder::Object(ObjectPublisher::new(broadcast.clone())),
			PublishFormat::Hls { playlist } => {
				let hls = import::Hls::new(
					broadcast.clone(),
					catalog.clone(),
					import::HlsConfig::new(playlist.clone()),
				)?;
				PublishDecoder::Hls(Box::new(hls))
			}
		};

		Ok(Self { decoder, broadcast })
	}

	pub fn consume(&self) -> moq_lite::BroadcastConsumer {
		self.broadcast.consume()
	}

	pub async fn run(mut self) -> anyhow::Result<()> {
		if let PublishDecoder::Object(decoder) = self.decoder {
			decoder.run().await
		} else if let PublishDecoder::Hls(decoder) = &mut self.decoder {
			decoder.init().await?;
			decoder.run().await
		} else {
			let mut stdin = tokio::io::stdin();
			let mut buffer = bytes::BytesMut::new();

			loop {
				let n = tokio::io::AsyncReadExt::read_buf(&mut stdin, &mut buffer).await?;
				if n == 0 {
					return Ok(());
				}
				self.decoder.decode_buf(&mut buffer)?;
			}
		}
	}
}
