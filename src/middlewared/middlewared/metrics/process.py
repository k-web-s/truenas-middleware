import psutil
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily


class ProcessCollector:
    def __init__(self):
        self._proc = psutil.Process()

    def collect(self):
        with self._proc.oneshot():
            cpu = self._proc.cpu_times()
            mem = self._proc.memory_info()
            fds = self._proc.num_fds()
            threads = self._proc.num_threads()
            start_time = self._proc.create_time()

        cpu_total = CounterMetricFamily(
            'process_cpu_seconds_total',
            'Total user and system CPU time spent in seconds',
        )
        cpu_total.add_metric([], cpu.user + cpu.system)
        yield cpu_total

        cpu_user = CounterMetricFamily(
            'process_cpu_seconds_user_total',
            'Total user CPU time spent in seconds',
        )
        cpu_user.add_metric([], cpu.user)
        yield cpu_user

        cpu_system = CounterMetricFamily(
            'process_cpu_seconds_system_total',
            'Total system CPU time spent in seconds',
        )
        cpu_system.add_metric([], cpu.system)
        yield cpu_system

        vmem = GaugeMetricFamily(
            'process_virtual_memory_bytes',
            'Virtual memory size in bytes',
        )
        vmem.add_metric([], mem.vms)
        yield vmem

        rss = GaugeMetricFamily(
            'process_resident_memory_bytes',
            'Resident memory size in bytes',
        )
        rss.add_metric([], mem.rss)
        yield rss

        start = GaugeMetricFamily(
            'process_start_time_seconds',
            'Start time of the process since unix epoch in seconds',
        )
        start.add_metric([], start_time)
        yield start

        thread_gauge = GaugeMetricFamily(
            'process_threads',
            'Number of OS threads',
        )
        thread_gauge.add_metric([], threads)
        yield thread_gauge

        open_fds = GaugeMetricFamily(
            'process_open_fds',
            'Number of open file descriptors',
        )
        open_fds.add_metric([], fds)
        yield open_fds

        try:
            soft, hard = self._proc.rlimit(psutil.RLIMIT_NOFILE)
            max_fds = GaugeMetricFamily(
                'process_max_fds',
                'Maximum number of open file descriptors',
            )
            max_fds.add_metric([], hard)
            yield max_fds
        except Exception:
            pass

