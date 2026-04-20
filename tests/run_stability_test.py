#!/usr/bin/env python3
"""
MOQ视频传输稳定性多次测试

运行多次测试，验证视频传输的稳定性。
"""

import asyncio
import hashlib
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tests.test_real_video_transfer import VideoTransferTest, TEST_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


async def run_multiple_tests(num_runs: int = 5):
    """
    运行多次视频传输测试

    Args:
        num_runs: 测试运行次数

    Returns:
        dict: 统计结果
    """
    results = []

    logger.info("=" * 70)
    logger.info(f"MOQ视频传输稳定性测试 - 运行 {num_runs} 次")
    logger.info("=" * 70)

    for i in range(num_runs):
        logger.info(f"\n{'=' * 70}")
        logger.info(f"测试运行 {i + 1}/{num_runs}")
        logger.info(f"{'=' * 70}")

        test = VideoTransferTest()
        try:
            success = await test.run_transmission_test()

            if success:
                result = test.analyze_results()
                test.print_results(result)
                results.append(
                    {
                        "run": i + 1,
                        "success": result["success"],
                        "hash_match": result["hash_match"],
                        "loss_rate": result["loss_rate"],
                        "duration": result["duration"],
                        "throughput_mbps": result["throughput_mbps"],
                        "original_hash": result["original_hash"],
                        "received_hash": result["received_hash"],
                    }
                )
            else:
                results.append(
                    {"run": i + 1, "success": False, "error": "传输测试执行失败"}
                )

        except Exception as e:
            logger.error(f"测试 {i + 1} 执行异常: {e}")
            import traceback

            traceback.print_exc()
            results.append({"run": i + 1, "success": False, "error": str(e)})

        finally:
            await test.cleanup()

    # 输出统计结果
    logger.info("\n" + "=" * 70)
    logger.info("测试统计结果")
    logger.info("=" * 70)

    success_count = sum(1 for r in results if r.get("success", False))
    fail_count = num_runs - success_count

    logger.info(f"总测试次数: {num_runs}")
    logger.info(f"成功次数: {success_count}")
    logger.info(f"失败次数: {fail_count}")
    logger.info(f"成功率: {success_count / num_runs * 100:.1f}%")

    if success_count > 0:
        avg_loss_rate = (
            sum(r.get("loss_rate", 0) for r in results if r.get("success"))
            / success_count
        )
        avg_duration = (
            sum(r.get("duration", 0) for r in results if r.get("success"))
            / success_count
        )
        avg_throughput = (
            sum(r.get("throughput_mbps", 0) for r in results if r.get("success"))
            / success_count
        )

        logger.info(f"平均丢包率: {avg_loss_rate * 100:.2f}%")
        logger.info(f"平均传输时间: {avg_duration:.3f} 秒")
        logger.info(f"平均吞吐量: {avg_throughput:.2f} Mbps")

    logger.info("-" * 70)
    logger.info("每次测试详情:")
    for r in results:
        if r.get("success"):
            logger.info(
                f"  测试 #{r['run']}: {'✓ 成功' if r['hash_match'] else '✗ 失败'} "
                f"(丢包率: {r['loss_rate'] * 100:.2f}%, 吞吐量: {r['throughput_mbps']:.2f} Mbps)"
            )
        else:
            logger.info(
                f"  测试 #{r['run']}: ✗ 失败 ({r.get('error', 'Unknown error')})"
            )

    logger.info("=" * 70)

    # 验证所有成功的测试是否具有相同的hash
    if success_count > 1:
        hashes = [
            r["original_hash"]
            for r in results
            if r.get("success") and r.get("original_hash")
        ]
        if len(set(hashes)) == 1:
            logger.info(f"✓ 所有成功测试的原始视频hash一致")
            logger.info(f"  Hash: {hashes[0]}")
        else:
            logger.warning(
                "! 不同测试运行的原始视频hash不一致（这是正常的，因为每次生成新的视频）"
            )

    return {
        "total_runs": num_runs,
        "success_count": success_count,
        "fail_count": fail_count,
        "success_rate": success_count / num_runs,
        "results": results,
    }


async def main():
    """主函数"""
    num_runs = 3  # 默认运行3次

    if len(sys.argv) > 1:
        try:
            num_runs = int(sys.argv[1])
        except ValueError:
            logger.error(f"无效的参数: {sys.argv[1]}")
            return 1

    stats = await run_multiple_tests(num_runs)

    return 0 if stats["success_rate"] == 1.0 else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
