use bytes::{Buf, BufMut};

use crate::coding::{Decode, DecodeError, Encode, EncodeError, Sizer};

use super::Version;

/// A trait for IETF messages that are automatically size-prefixed during encoding/decoding.
///
/// IETF messages have a size prefix and a message type ID for control stream dispatch.
pub trait Message: Sized + std::fmt::Debug {
	const ID: u64;

	/// Encode this message body (without size prefix).
	fn encode_msg<W: BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError>;

	/// Decode a message body (without size prefix).
	fn decode_msg<B: Buf>(buf: &mut B, version: Version) -> Result<Self, DecodeError>;
}

pub(crate) fn encode_message_size<W: BufMut>(w: &mut W, size: usize, version: Version) -> Result<(), EncodeError> {
	match version {
		Version::Draft17 => (size as u64).encode(w, version),
		Version::Draft14 | Version::Draft15 | Version::Draft16 => u16::try_from(size)
			.map_err(|_| EncodeError::TooLarge)?
			.encode(w, version),
	}
}

pub(crate) fn decode_message_size<B: Buf>(buf: &mut B, version: Version) -> Result<usize, DecodeError> {
	match version {
		Version::Draft17 => usize::decode(buf, version),
		Version::Draft14 | Version::Draft15 | Version::Draft16 => Ok(u16::decode(buf, version)? as usize),
	}
}

impl<T: Message> Encode<Version> for T {
	fn encode<W: BufMut>(&self, w: &mut W, version: Version) -> Result<(), EncodeError> {
		tracing::trace!(?self, "encoding");
		let mut sizer = Sizer::default();
		self.encode_msg(&mut sizer, version)?;
		encode_message_size(w, sizer.size, version)?;
		self.encode_msg(w, version)
	}
}

impl<T: Message> Decode<Version> for T {
	fn decode<B: Buf>(buf: &mut B, version: Version) -> Result<Self, DecodeError> {
		let size = decode_message_size(buf, version)?;

		if tracing::enabled!(tracing::Level::TRACE) {
			if buf.remaining() < size {
				return Err(DecodeError::Short);
			}
			let raw = buf.copy_to_bytes(size);
			let mut slice = &raw[..];
			match Self::decode_msg(&mut slice, version) {
				Ok(result) => {
					if slice.remaining() > 0 {
						return Err(DecodeError::Long);
					}
					tracing::trace!(?result, "decoded");
					Ok(result)
				}
				Err(e) => {
					tracing::warn!(%e, ?raw, "decode failed");
					Err(e)
				}
			}
		} else {
			if buf.remaining() < size {
				return Err(DecodeError::Short);
			}
			let mut limited = buf.take(size);
			match Self::decode_msg(&mut limited, version) {
				Ok(result) => {
					if limited.remaining() > 0 {
						return Err(DecodeError::Long);
					}
					Ok(result)
				}
				Err(e) => {
					tracing::warn!(%e, "decode failed");
					Err(e)
				}
			}
		}
	}
}
