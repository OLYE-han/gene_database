"""Process-wide interpreter settings applied before query/service startup.

All engines and data services hosted in one Python process must use the same
runtime configuration. The switch interval is not an engine-local property:
closing one engine does not restore it and queries never change it. Configure
different intervals in separate processes for comparisons.
"""
import math
import sys


def apply_process_runtime(c):
    milliseconds = c['runtime']['python_switch_interval_ms']
    if (type(milliseconds) not in (int, float) or not math.isfinite(milliseconds)
            or milliseconds <= 0 or milliseconds / 1000 <= 0):
        raise ValueError('runtime.python_switch_interval_ms must be finite and positive')
    sys.setswitchinterval(milliseconds / 1000)
    from .gc_profile import configure
    configure(c.get('metrics', {}).get('gc_trace', False),
              c.get('metrics', {}).get('resource_max_samples', 1024))
    return {'python_switch_interval_ms': sys.getswitchinterval() * 1000}
