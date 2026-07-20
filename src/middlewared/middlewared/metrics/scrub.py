import threading

import libzfs
from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily

from . import lock_failures


class ScrubCollector:
    def __init__(self):
        self._lock = threading.Lock()

    def collect(self):
        if not self._lock.acquire(timeout=2):
            lock_failures.labels(collector='scrub').inc()
            return

        try:
            bytes_to_scan = GaugeMetricFamily(
                'zpool_scrub_bytes_to_scan',
                'SCRUB bytes to scan',
                labels=['pool', 'function'],
            )
            bytes_scanned = CounterMetricFamily(
                'zpool_scrub_bytes_scanned',
                'SCRUB bytes scanned',
                labels=['pool', 'function'],
            )
            bytes_issued = CounterMetricFamily(
                'zpool_scrub_bytes_issued',
                'SCRUB bytes issued',
                labels=['pool', 'function'],
            )

            with libzfs.ZFS() as zfs:
                for pool in zfs.pools:
                    scrub = pool.scrub
                    if scrub.state == libzfs.ScanState.SCANNING:
                        label_values = [pool.name, scrub.function.name]
                        bytes_to_scan.add_metric(label_values, scrub.bytes_to_scan)
                        bytes_scanned.add_metric(label_values, scrub.bytes_scanned)
                        bytes_issued.add_metric(label_values, scrub.bytes_issued)

            yield bytes_to_scan
            yield bytes_scanned
            yield bytes_issued
        finally:
            self._lock.release()
