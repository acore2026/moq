#!/usr/bin/env python3
"""
Deprecated compatibility wrapper.

Use `examples/integration_example.py` for the maintained external-usage
reference. This wrapper keeps the older path usable for now.
"""

import asyncio
import logging

from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()
logger = logging.getLogger(__name__)

from integration_example import main as integration_main


async def main():
    logger.info(
        "This example is deprecated. Run examples/integration_example.py instead."
    )
    await integration_main()


if __name__ == "__main__":
    asyncio.run(main())
