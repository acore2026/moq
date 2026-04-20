#!/usr/bin/env python3
"""
Test runner for MOQ Transport tests.

Usage:
    python run_tests.py                          # Run all tests
    python run_tests.py -v                       # Run with verbose output
    python run_tests.py -k test_01               # Run specific test
    python run_tests.py --quick                  # Run quick tests only
"""

import argparse
import asyncio
import logging
import sys
import unittest
from pathlib import Path

# Add parent directory to path for moq module
# The actual module is in 'moq-modified' folder, we use a symlink 'moq'
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir.parent))  # /home/acn/zqm
# Verify moq module is available
try:
    import moq
except ImportError:
    # Create symlink if needed
    moq_path = script_dir.parent / "moq"
    if not moq_path.exists():
        import os

        os.symlink(str(script_dir), str(moq_path))
    import moq

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def discover_tests(quick_only=False):
    """Discover all tests."""
    loader = unittest.TestLoader()
    start_dir = Path(__file__).parent / "tests"

    if quick_only:
        # Only load specific quick tests
        suite = unittest.TestSuite()
        from tests.test_connection_stability import TestConnectionStability
        from tests.test_edge_cases import TestEdgeCases

        # Add quick tests
        quick_tests = [
            TestConnectionStability("test_01_small_data_stream_mode"),
            TestConnectionStability("test_02_small_data_datagram_mode"),
            TestEdgeCases("test_zero_byte_payload"),
            TestEdgeCases("test_multiple_subscribers_same_track"),
        ]
        suite.addTests(quick_tests)
        return suite
    else:
        return loader.discover(str(start_dir), pattern="test_*.py")


def run_tests(tests):
    """Run tests with proper async support."""
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(tests)
    return result.wasSuccessful()


def main():
    parser = argparse.ArgumentParser(description="Run MOQ Transport tests")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-k", "--pattern", type=str, help="Test pattern to match")
    parser.add_argument("--quick", action="store_true", help="Run quick tests only")
    parser.add_argument("--failfast", action="store_true", help="Stop on first failure")
    parser.add_argument("--list", action="store_true", help="List all tests")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.list:
        print("Available tests:")
        print()
        print("Connection Stability Tests:")
        print("  test_01_small_data_stream_mode     - Basic stream mode test")
        print("  test_02_small_data_datagram_mode   - Basic datagram mode test")
        print("  test_03_long_interval_transmission - 20s idle period test")
        print("  test_04_video_stream_mode          - Video stream test")
        print("  test_05_video_datagram_mode        - Video datagram test")
        print("  test_06_rapid_connect_disconnect   - Connection stress test")
        print()
        print("Edge Case Tests:")
        print("  test_multiple_subscribers_same_track         - Multiple subscribers")
        print("  test_subscriber_disconnect_before_unsubscribe - Abrupt disconnect")
        print("  test_publisher_disconnect_while_publishing   - Mid-publish disconnect")
        print("  test_zero_byte_payload                       - Empty payload handling")
        print("  test_large_payload                           - Large data handling")
        print(
            "  test_many_tracks                             - Multiple track handling"
        )
        print("  test_reconnect_after_disconnect              - Reconnection handling")
        print()
        print("Potential Bug Tests:")
        print("  test_stream_buffer_cleanup       - Buffer cleanup verification")
        print("  test_subscription_list_cleanup   - Subscription cleanup verification")
        print("  test_idle_timeout_detection      - Idle timeout detection")
        return 0

    # Discover tests
    if args.quick:
        logger.info("Running quick tests only...")
        tests = discover_tests(quick_only=True)
    else:
        tests = discover_tests()

    # Filter by pattern if specified
    if args.pattern:
        loader = unittest.TestLoader()
        filtered = unittest.TestSuite()
        for test in tests:
            if args.pattern in str(test):
                filtered.addTest(test)
        tests = filtered
        logger.info(f"Running tests matching pattern: {args.pattern}")

    # Run tests
    logger.info("Starting test run...")
    success = run_tests(tests)

    if success:
        logger.info("All tests passed!")
        return 0
    else:
        logger.error("Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
