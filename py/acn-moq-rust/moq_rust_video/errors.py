"""
Exceptions raised by moq_rust_video.
"""


class MoqRustVideoError(RuntimeError):
    """Base exception for Rust MoQ video wrapper failures."""


class BinaryNotFoundError(MoqRustVideoError):
    """Raised when a required external or bundled binary cannot be found."""


class ProcessStartError(MoqRustVideoError):
    """Raised when ffmpeg or moq-cli cannot be started."""


class ProcessExitedError(MoqRustVideoError):
    """Raised when a managed process exits unexpectedly."""


class FrameDecodeError(MoqRustVideoError):
    """Raised when an AVC3 subscriber frame cannot be decoded."""
