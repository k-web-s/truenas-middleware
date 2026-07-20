import threading

import libzfs
from prometheus_client.core import GaugeMetricFamily

from . import lock_failures

SPACE_ATTRS = [
    'used', 'available', 'referenced', 'compressratio',
    'refreservation', 'usedbysnapshots', 'usedbydataset',
    'usedbychildren', 'usedbyrefreservation', 'refcompressratio',
    'written', 'logicalused', 'logicalreferenced',
]


class SpaceCollector:
    def __init__(self):
        self._lock = threading.Lock()

    def collect(self):
        if not self._lock.acquire(timeout=2):
            lock_failures.labels(collector='space').inc()
            return

        try:
            families = {
                attr: GaugeMetricFamily(
                    f'zfs_dataset_{attr}',
                    f'{attr} attribute',
                    labels=['name', 'type', 'comments'],
                )
                for attr in SPACE_ATTRS
            }

            with libzfs.ZFS() as zfs:
                for ds in _flatten(zfs.datasets_serialized(
                    props=SPACE_ATTRS, user_props=True,
                )):
                    for attr in SPACE_ATTRS:
                        families[attr].add_metric(
                            [
                                ds['name'],
                                ds['type'],
                                ds['properties'].get(
                                    'org.freenas:description', {}
                                ).get('value', ''),
                            ],
                            float(ds['properties'][attr]['rawvalue']),
                        )

            yield from families.values()
        finally:
            self._lock.release()


def _flatten(datasets):
    for ds in datasets:
        yield ds
        yield from _flatten(ds.pop('children', []))
