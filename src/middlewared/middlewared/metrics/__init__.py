import logging
import os
import shutil
import threading
import time

import yaml

logger = logging.getLogger(__name__)

lock_failures = None


def setup():
    config_path = '/data/middlewared.metrics.yaml'
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning('Metrics config %s not found, skipping setup', config_path)
        return
    except Exception as e:
        logger.warning('Failed to read metrics config: %s', e)
        return

    if not config:
        return

    metrics_addr = config.get('address')
    if not metrics_addr:
        return

    from prometheus_client import Counter

    global lock_failures
    lock_failures = Counter(
        'zfs_collect_lock_failures_total',
        'Total number of times the ZFS collect lock could not be acquired',
        ['collector'],
    )

    metrics_dir = '/var/tmp/middlewared/metrics'
    os.environ['PROMETHEUS_MULTIPROC_DIR'] = metrics_dir
    shutil.rmtree(metrics_dir, ignore_errors=True)
    os.makedirs(metrics_dir, exist_ok=True)

    from prometheus_client import start_http_server
    from prometheus_client.core import REGISTRY

    from prometheus_client.multiprocess import MultiProcessCollector
    MultiProcessCollector(registry=REGISTRY)

    from .scrub import ScrubCollector
    REGISTRY.register(ScrubCollector())

    from .space import SpaceCollector
    REGISTRY.register(SpaceCollector())

    from prometheus_client import PROCESS_COLLECTOR
    REGISTRY.unregister(PROCESS_COLLECTOR)

    from .process import ProcessCollector
    REGISTRY.register(ProcessCollector())

    host, port = metrics_addr.rsplit(':', 1)
    host = host or '0.0.0.0'

    def _start_with_retry():
        delay = 1
        while True:
            try:
                start_http_server(int(port), addr=host)
            except OSError:
                logger.warning(
                    'Prometheus HTTP server failed to bind to %s:%s, '
                    'retrying in %ds', host, port, delay
                )
                time.sleep(delay)
                delay = min(delay * 2, 30)
            else:
                logger.info('Prometheus HTTP server listening on %s:%s', host, port)
                return

    t = threading.Thread(target=_start_with_retry, daemon=True)
    t.start()
