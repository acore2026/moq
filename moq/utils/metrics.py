"""
Metrics collection for MOQ
"""

import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import asyncio


@dataclass
class MetricPoint:
    """Single metric data point"""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Metrics collector for MOQ performance monitoring"""
    
    def __init__(self, max_history: int = 10000):
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[MetricPoint]] = defaultdict(list)
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._max_history = max_history
        self._lock = asyncio.Lock()
    
    async def increment(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric"""
        async with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] += value
    
    async def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric"""
        async with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value
    
    async def record_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram value"""
        async with self._lock:
            point = MetricPoint(timestamp=time.time(), value=value, labels=labels or {})
            self._histograms[name].append(point)
            
            # Trim history if needed
            if len(self._histograms[name]) > self._max_history:
                self._histograms[name] = self._histograms[name][-self._max_history:]
    
    async def time_operation(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations"""
        start_time = time.time()
        try:
            yield
        finally:
            elapsed = time.time() - start_time
            await self.record_histogram(name, elapsed * 1000, labels)  # ms
    
    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create metric key with labels"""
        if labels:
            label_str = ','.join([f"{k}={v}" for k, v in sorted(labels.items())])
            return f"{name}{{{label_str}}}"
        return name
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get all metrics statistics"""
        async with self._lock:
            stats = {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histograms': {},
            }
            
            for name, points in self._histograms.items():
                if points:
                    values = [p.value for p in points]
                    stats['histograms'][name] = {
                        'count': len(values),
                        'min': min(values),
                        'max': max(values),
                        'avg': sum(values) / len(values),
                        'p50': sorted(values)[len(values) // 2],
                        'p95': sorted(values)[int(len(values) * 0.95)] if len(values) > 20 else max(values),
                    }
            
            return stats
    
    async def reset(self) -> None:
        """Reset all metrics"""
        async with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
