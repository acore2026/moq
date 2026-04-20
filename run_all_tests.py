#!/usr/bin/env python3
"""
Comprehensive test runner for MOQ Transport.

Runs all test suites and generates a summary report.
"""

import argparse
import asyncio
import logging
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_tests(test_pattern: str = "test_*.py", verbose: bool = False):
    """Run all tests matching pattern."""
    loader = unittest.TestLoader()
    start_dir = Path(__file__).parent / "tests"

    # Discover tests
    suite = loader.discover(str(start_dir), pattern=test_pattern)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
    result = runner.run(suite)

    return result.wasSuccessful()


def run_quick_tests():
    """Run quick tests only."""
    from tests.test_simple import TestSimple
    from tests.test_file_transfer import TestFileTransfer

    suite = unittest.TestSuite()

    # Add basic tests
    suite.addTest(TestSimple("test_basic_stream"))
    suite.addTest(TestSimple("test_basic_datagram"))
    suite.addTest(TestFileTransfer("test_01_small_file_transfer_stream"))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_video_tests():
    """Run video scenario tests."""
    from tests.test_video_scenarios import TestVideoScenarios

    suite = unittest.TestLoader().loadTestsFromTestCase(TestVideoScenarios)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_file_tests():
    """Run file transfer tests."""
    from tests.test_file_transfer import TestFileTransfer

    suite = unittest.TestLoader().loadTestsFromTestCase(TestFileTransfer)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_stress_tests():
    """Run stress tests."""
    from tests.test_stress import TestStress

    suite = unittest.TestLoader().loadTestsFromTestCase(TestStress)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_long_running_tests():
    """Run long running tests."""
    from tests.test_long_running import TestLongRunning

    suite = unittest.TestLoader().loadTestsFromTestCase(TestLongRunning)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def main():
    parser = argparse.ArgumentParser(description="Run MOQ Transport tests")
    parser.add_argument("--quick", action="store_true", help="Run quick tests only")
    parser.add_argument("--video", action="store_true", help="Run video tests")
    parser.add_argument("--file", action="store_true", help="Run file transfer tests")
    parser.add_argument("--stress", action="store_true", help="Run stress tests")
    parser.add_argument("--long", action="store_true", help="Run long running tests")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--pattern", type=str, default="test_*.py", help="Test file pattern"
    )

    args = parser.parse_args()

    success = True

    if args.quick:
        logger.info("Running quick tests...")
        success = run_quick_tests()
    elif args.video:
        logger.info("Running video tests...")
        success = run_video_tests()
    elif args.file:
        logger.info("Running file tests...")
        success = run_file_tests()
    elif args.stress:
        logger.info("Running stress tests...")
        success = run_stress_tests()
    elif args.long:
        logger.info("Running long running tests...")
        success = run_long_running_tests()
    elif args.all:
        logger.info("Running all tests...")
        success = run_tests(args.pattern, args.verbose)
    else:
        # Default: run quick tests
        logger.info("Running quick tests (use --all for full test suite)...")
        success = run_quick_tests()

    if success:
        logger.info("All tests passed!")
        return 0
    else:
        logger.error("Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
