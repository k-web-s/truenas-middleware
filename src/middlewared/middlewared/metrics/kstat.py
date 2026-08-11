import re
from collections import defaultdict

import sysctl
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

_sysctl_numeric_types = {
    sysctl.CTLTYPE_INT,
    sysctl.CTLTYPE_UINT,
    sysctl.CTLTYPE_LONG,
    sysctl.CTLTYPE_ULONG,
    sysctl.CTLTYPE_S64,
    sysctl.CTLTYPE_U64,
}

SYSCTL_RE = re.compile(r'^kstat\.zfs\..*?\.dataset\.objset-(.*?)\.(.*?)$')

MISC_PREFIX = 'kstat.zfs.misc.'

KSTAT_COUNTERS = {
    'nunlinked': ('freebsd_kstat_zfs_nunlinked', 'nunlinked'),
    'nunlinks': ('freebsd_kstat_zfs_nunlinks', 'nunlinks'),
    'nread': ('freebsd_kstat_zfs_nread', 'Bytes read'),
    'reads': ('freebsd_kstat_zfs_reads', 'Read operations'),
    'nwritten': ('freebsd_kstat_zfs_nwritten', 'Bytes written'),
    'writes': ('freebsd_kstat_zfs_writes', 'Write operations'),
}

MISC_COUNTERS = {
    'kstat.zfs.misc.zstd.decompress_failed',
    'kstat.zfs.misc.zstd.compress_failed',
    'kstat.zfs.misc.zstd.decompress_header_invalid',
    'kstat.zfs.misc.zstd.decompress_level_invalid',
    'kstat.zfs.misc.zstd.compress_level_invalid',
    'kstat.zfs.misc.zstd.decompress_alloc_fail',
    'kstat.zfs.misc.zstd.compress_alloc_fail',
    'kstat.zfs.misc.zstd.alloc_fallback',
    'kstat.zfs.misc.zstd.alloc_fail',
    'kstat.zfs.misc.vdev_mirror_stats.preferred_not_found',
    'kstat.zfs.misc.vdev_mirror_stats.preferred_found',
    'kstat.zfs.misc.vdev_mirror_stats.non_rotating_seek',
    'kstat.zfs.misc.vdev_mirror_stats.non_rotating_linear',
    'kstat.zfs.misc.vdev_mirror_stats.rotating_seek',
    'kstat.zfs.misc.vdev_mirror_stats.rotating_offset',
    'kstat.zfs.misc.vdev_mirror_stats.rotating_linear',
    'kstat.zfs.misc.vdev_cache_stats.misses',
    'kstat.zfs.misc.vdev_cache_stats.hits',
    'kstat.zfs.misc.vdev_cache_stats.delegations',
    'kstat.zfs.misc.zil.zil_itx_metaslab_slog_bytes',
    'kstat.zfs.misc.zil.zil_itx_metaslab_slog_count',
    'kstat.zfs.misc.zil.zil_itx_metaslab_normal_bytes',
    'kstat.zfs.misc.zil.zil_itx_metaslab_normal_count',
    'kstat.zfs.misc.zil.zil_itx_needcopy_bytes',
    'kstat.zfs.misc.zil.zil_itx_needcopy_count',
    'kstat.zfs.misc.zil.zil_itx_copied_bytes',
    'kstat.zfs.misc.zil.zil_itx_copied_count',
    'kstat.zfs.misc.zil.zil_itx_indirect_bytes',
    'kstat.zfs.misc.zil.zil_itx_indirect_count',
    'kstat.zfs.misc.zil.zil_itx_count',
    'kstat.zfs.misc.zil.zil_commit_writer_count',
    'kstat.zfs.misc.zil.zil_commit_count',
    'kstat.zfs.misc.dbufstats.metadata_cache_overflow',
    'kstat.zfs.misc.dbufstats.hash_insert_race',
    'kstat.zfs.misc.dbufstats.hash_collisions',
    'kstat.zfs.misc.dbufstats.hash_misses',
    'kstat.zfs.misc.dbufstats.hash_hits',
    'kstat.zfs.misc.dbufstats.cache_total_evicts',
    'kstat.zfs.misc.arcstats.demand_hit_prescient_prefetch',
    'kstat.zfs.misc.arcstats.demand_hit_predictive_prefetch',
    'kstat.zfs.misc.arcstats.l2_rebuild_log_blks',
    'kstat.zfs.misc.arcstats.l2_rebuild_bufs_precached',
    'kstat.zfs.misc.arcstats.l2_rebuild_bufs',
    'kstat.zfs.misc.arcstats.l2_rebuild_lowmem',
    'kstat.zfs.misc.arcstats.l2_rebuild_cksum_lb_errors',
    'kstat.zfs.misc.arcstats.l2_rebuild_dh_errors',
    'kstat.zfs.misc.arcstats.l2_rebuild_io_errors',
    'kstat.zfs.misc.arcstats.l2_rebuild_unsupported',
    'kstat.zfs.misc.arcstats.l2_rebuild_success',
    'kstat.zfs.misc.arcstats.l2_data_to_meta_ratio',
    'kstat.zfs.misc.arcstats.l2_log_blk_writes',
    'kstat.zfs.misc.arcstats.l2_io_error',
    'kstat.zfs.misc.arcstats.l2_cksum_bad',
    'kstat.zfs.misc.arcstats.l2_abort_lowmem',
    'kstat.zfs.misc.arcstats.l2_free_on_write',
    'kstat.zfs.misc.arcstats.l2_evict_l1cached',
    'kstat.zfs.misc.arcstats.l2_evict_reading',
    'kstat.zfs.misc.arcstats.l2_evict_lock_retry',
    'kstat.zfs.misc.arcstats.l2_writes_lock_retry',
    'kstat.zfs.misc.arcstats.l2_writes_error',
    'kstat.zfs.misc.arcstats.l2_writes_done',
    'kstat.zfs.misc.arcstats.l2_writes_sent',
    'kstat.zfs.misc.arcstats.l2_write_bytes',
    'kstat.zfs.misc.arcstats.l2_read_bytes',
    'kstat.zfs.misc.arcstats.l2_rw_clash',
    'kstat.zfs.misc.arcstats.l2_feeds',
    'kstat.zfs.misc.arcstats.l2_misses',
    'kstat.zfs.misc.arcstats.l2_hits',
    'kstat.zfs.misc.arcstats.evict_l2_skip',
    'kstat.zfs.misc.arcstats.evict_l2_ineligible',
    'kstat.zfs.misc.arcstats.evict_l2_eligible_mru',
    'kstat.zfs.misc.arcstats.evict_l2_eligible_mfu',
    'kstat.zfs.misc.arcstats.evict_l2_eligible',
    'kstat.zfs.misc.arcstats.evict_l2_cached',
    'kstat.zfs.misc.arcstats.evict_not_enough',
    'kstat.zfs.misc.arcstats.evict_skip',
    'kstat.zfs.misc.arcstats.access_skip',
    'kstat.zfs.misc.arcstats.mutex_miss',
    'kstat.zfs.misc.arcstats.deleted',
    'kstat.zfs.misc.arcstats.mfu_ghost_hits',
    'kstat.zfs.misc.arcstats.mfu_hits',
    'kstat.zfs.misc.arcstats.mru_ghost_hits',
    'kstat.zfs.misc.arcstats.mru_hits',
    'kstat.zfs.misc.arcstats.prefetch_metadata_misses',
    'kstat.zfs.misc.arcstats.prefetch_metadata_hits',
    'kstat.zfs.misc.arcstats.prefetch_data_misses',
    'kstat.zfs.misc.arcstats.prefetch_data_hits',
    'kstat.zfs.misc.arcstats.demand_metadata_misses',
    'kstat.zfs.misc.arcstats.demand_metadata_hits',
    'kstat.zfs.misc.arcstats.demand_data_misses',
    'kstat.zfs.misc.arcstats.demand_data_hits',
    'kstat.zfs.misc.arcstats.misses',
    'kstat.zfs.misc.arcstats.hits',
    'kstat.zfs.misc.dmu_tx.dmu_tx_quota',
    'kstat.zfs.misc.dmu_tx.dmu_tx_wrlog_delay',
    'kstat.zfs.misc.dmu_tx.dmu_tx_dirty_frees_delay',
    'kstat.zfs.misc.dmu_tx.dmu_tx_dirty_over_max',
    'kstat.zfs.misc.dmu_tx.dmu_tx_dirty_delay',
    'kstat.zfs.misc.dmu_tx.dmu_tx_dirty_throttle',
    'kstat.zfs.misc.dmu_tx.dmu_tx_memory_reclaim',
    'kstat.zfs.misc.dmu_tx.dmu_tx_memory_reserve',
    'kstat.zfs.misc.dmu_tx.dmu_tx_group',
    'kstat.zfs.misc.dmu_tx.dmu_tx_suspended',
    'kstat.zfs.misc.dmu_tx.dmu_tx_error',
    'kstat.zfs.misc.dmu_tx.dmu_tx_delay',
    'kstat.zfs.misc.dmu_tx.dmu_tx_assigned',
    'kstat.zfs.misc.zfetchstats.io_active',
    'kstat.zfs.misc.zfetchstats.io_issued',
    'kstat.zfs.misc.zfetchstats.max_streams',
    'kstat.zfs.misc.zfetchstats.misses',
    'kstat.zfs.misc.zfetchstats.hits',
    'kstat.zfs.misc.dnodestats.dnode_move_active',
    'kstat.zfs.misc.dnodestats.dnode_move_rwlock',
    'kstat.zfs.misc.dnodestats.dnode_move_handle',
    'kstat.zfs.misc.dnodestats.dnode_move_special',
    'kstat.zfs.misc.dnodestats.dnode_move_recheck2',
    'kstat.zfs.misc.dnodestats.dnode_move_recheck1',
    'kstat.zfs.misc.dnodestats.dnode_move_invalid',
    'kstat.zfs.misc.dnodestats.dnode_alloc_next_block',
    'kstat.zfs.misc.dnodestats.dnode_alloc_race',
    'kstat.zfs.misc.dnodestats.dnode_alloc_next_chunk',
    'kstat.zfs.misc.dnodestats.dnode_buf_evict',
    'kstat.zfs.misc.dnodestats.dnode_reallocate',
    'kstat.zfs.misc.dnodestats.dnode_allocate',
    'kstat.zfs.misc.dnodestats.dnode_free_interior_lock_retry',
    'kstat.zfs.misc.dnodestats.dnode_hold_free_refcount',
    'kstat.zfs.misc.dnodestats.dnode_hold_free_overflow',
    'kstat.zfs.misc.dnodestats.dnode_hold_free_lock_retry',
    'kstat.zfs.misc.dnodestats.dnode_hold_free_lock_misses',
    'kstat.zfs.misc.dnodestats.dnode_hold_free_misses',
    'kstat.zfs.misc.dnodestats.dnode_hold_free_hits',
    'kstat.zfs.misc.dnodestats.dnode_hold_alloc_type_none',
    'kstat.zfs.misc.dnodestats.dnode_hold_alloc_lock_misses',
    'kstat.zfs.misc.dnodestats.dnode_hold_alloc_lock_retry',
    'kstat.zfs.misc.dnodestats.dnode_hold_alloc_interior',
    'kstat.zfs.misc.dnodestats.dnode_hold_alloc_misses',
    'kstat.zfs.misc.dnodestats.dnode_hold_alloc_hits',
    'kstat.zfs.misc.dnodestats.dnode_hold_dbuf_read',
    'kstat.zfs.misc.dnodestats.dnode_hold_dbuf_hold',
    'kstat.zfs.misc.metaslab_stats.try_hard',
    'kstat.zfs.misc.metaslab_stats.too_many_tries',
    'kstat.zfs.misc.metaslab_stats.reload_tree',
    'kstat.zfs.misc.metaslab_stats.trace_over_limit',
    'kstat.zfs.misc.fm.erpt-duplicates',
    'kstat.zfs.misc.fm.payload-set-failed',
    'kstat.zfs.misc.fm.fmri-set-failed',
    'kstat.zfs.misc.fm.erpt-set-failed',
    'kstat.zfs.misc.fm.erpt-dropped',
}


def _sanitize_name(key):
    return 'freebsd_' + key.replace('.', '_').replace('-', '_')


class KstatCollector:
    def __init__(self):
        self._descriptions = {}
        for ctl in sysctl.filter(MISC_PREFIX.rstrip('.')):
            self._descriptions[ctl.name] = getattr(ctl, 'description', None) or ctl.name

    def collect(self):
        dataset_stats = defaultdict(dict)

        for ctl in sysctl.filter('kstat.zfs'):
            name = ctl.name
            if name.startswith(MISC_PREFIX):
                if ctl.type not in _sysctl_numeric_types:
                    continue
                metric_name = _sanitize_name(name)
                desc = self._descriptions.get(name, getattr(ctl, 'description', None) or name)
                cls = CounterMetricFamily if name in MISC_COUNTERS else GaugeMetricFamily
                family = cls(metric_name, desc, labels=[])
                family.add_metric([], float(ctl.value))
                yield family
            elif match := SYSCTL_RE.match(name):
                dataset_stats[match.group(1)][match.group(2)] = ctl.value

        families = {
            metric_name: CounterMetricFamily(metric_name, help_, labels=['dataset'])
            for metric_name, help_ in KSTAT_COUNTERS.values()
        }

        for objset, values in dataset_stats.items():
            dataset = values.get('dataset_name')

            for field, (metric_name, _) in KSTAT_COUNTERS.items():
                if field in values:
                    families[metric_name].add_metric([dataset], float(values[field]))

        yield from families.values()
