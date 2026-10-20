"""Per-query detailed tracing; aggregate query metrics are always retained."""
def trace_query(c, query_id):
    metrics=c['metrics']
    return metrics['detailed_trace'] or query_id in metrics.get('trace_query_ids', [])


def trace_lifecycle(c, query_id):
    """Small branch/head timing records, independent of per-read detail."""
    return c['metrics'].get('lifecycle_trace', False) or trace_query(c, query_id)
