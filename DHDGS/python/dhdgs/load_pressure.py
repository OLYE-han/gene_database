"""Search-ready pressure, separate from admission and CPU utilization."""
import math


def quantile_bounds(hist, q):
    count = sum(hist)
    if not count:
        return None
    threshold = math.ceil(count * q)
    cumulative = 0
    for bucket, n in enumerate(hist):
        cumulative += n
        if cumulative >= threshold:
            return (0. if bucket == 0 else 2. ** bucket / 1000,
                    2. ** (bucket + 1) / 1000)


def search_pressure(current, previous, service_ms=1.):
    """Cumulative counters -> one window; no completions is not zero wait.

    Bucket bounds are conservative, not interpolated percentiles. Old runtimes
    expose accepted-to-worker latency and are explicitly labelled as such.
    A restart/reset invalidates this window instead of manufacturing low load.
    """
    key = 'compute_ready_wait_hist_us_log2'
    ready = key in current and key in previous
    if not ready:
        key = 'compute_wait_hist_us_log2'
    a, b = current[key], previous[key]
    if len(a) != len(b) or any(x < y for x, y in zip(a, b)):
        raise ValueError('Search pressure counters reset')
    completed = current['compute_completed'] - previous['compute_completed']
    work = current['compute_wall_ns'] - previous['compute_wall_ns']
    if completed < 0 or work < 0:
        raise ValueError('Search service counters reset')
    mean = work / 1e6 / completed if completed else service_ms
    hist = [x-y for x, y in zip(a, b)]
    queue = current['compute_queue']
    workers = queue['workers']
    if workers <= 0:
        raise ValueError('No search workers')
    # queued is already grants in region mode, actors in legacy mode. Taking
    # max avoids double counting and handles legacy non-region snapshots.
    pending = max(queue['queued'], current.get('region_ready_grants', 0))
    backlog = pending * mean / workers
    bounds = {f'queue_p{q}_ms_bounds': quantile_bounds(hist, q/100)
              for q in (50, 95, 99)}
    p95 = bounds['queue_p95_ms_bounds']
    result = dict(**bounds, queue_p95_ms=p95[1] if p95 else None,
                queue_ms=max(p95[1] if p95 else 0., backlog),
                queue_time_basis='ready_to_worker' if ready else 'accepted_to_worker_legacy',
                queue_started_samples=sum(hist), ready_grants=pending,
                queued_actors=queue.get('queued_region_executors', queue['queued']),
                backlog_estimate_ms=backlog, service_ms=mean)
    for prefix, key in [('runnable', 'compute_executor_wait_hist_us_log2'),
                        ('actor_serial', 'compute_actor_wait_hist_us_log2')]:
        if key not in current or key not in previous:
            continue
        a, b = current[key], previous[key]
        if len(a) != len(b) or any(x < y for x, y in zip(a, b)):
            raise ValueError('Search decomposition counters reset')
        delta = [x-y for x,y in zip(a,b)]
        result[prefix+'_p95_ms_bounds'] = quantile_bounds(delta, .95)
    fallback = current.get('ready_clock_fallbacks', 0)-previous.get('ready_clock_fallbacks', 0)
    if fallback < 0:
        raise ValueError('Search clock counters reset')
    result['ready_clock_fallbacks'] = fallback
    if fallback:
        result['queue_time_basis'] = 'mixed_ready_and_fallback'
    # Node capacity differs from per-query serial dependence. Expose the
    # runnable pressure independently; old controllers keep their total wait
    # field until a queue-driven policy is validated on changing workloads.
    if 'runnable_p95_ms_bounds' in result and not fallback:
        bounds = result['runnable_p95_ms_bounds']
        result['worker_backlog_estimate_ms'] = result['queued_actors'] * mean / workers
        result['worker_pressure_ms'] = max(bounds[1] if bounds else 0.,
                                           result['worker_backlog_estimate_ms'])
    else:
        result['worker_pressure_ms'] = None
    return result
