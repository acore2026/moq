"""
Logging utilities for MOQ
"""

import logging
import sys
from typing import Optional
from pathlib import Path
from datetime import datetime


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    console_output: bool = True,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Setup logging for MOQ application.
    
    Args:
        level: Logging level
        log_file: Optional file to log to
        console_output: Whether to output to console
        format_string: Custom format string
    
    Returns:
        Root logger
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create formatter
    formatter = logging.Formatter(format_string)
    
    # Setup root logger
    root_logger = logging.getLogger('moq')
    root_logger.setLevel(level)
    
    # Remove existing handlers
    root_logger.handlers = []
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger


class ContextAdapter(logging.LoggerAdapter):
    """Logger adapter with context information"""
    
    def __init__(self, logger: logging.Logger, context: dict):
        super().__init__(logger, context)
    
    def process(self, msg, kwargs):
        context_str = ' '.join([f"[{k}={v}]" for k, v in self.extra.items()])
        return f"{context_str} {msg}", kwargs


def get_logger(name: str, **context) -> logging.Logger:
    """Get a logger with optional context"""
    logger = logging.getLogger(f'moq.{name}')
    if context:
        return ContextAdapter(logger, context)
    return logger
