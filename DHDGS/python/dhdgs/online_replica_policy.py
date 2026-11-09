"""Elapsed-time hotspot persistence and bounded online layout selection.

Only the maintenance controller calls this module. It cannot dispatch a search
or alter a graph. Unknown pressure freezes evidence instead of counting as cold.
"""
from collections import deque
from dataclasses import dataclass, field
import math


def held_out(query_id, modulus):
    # Stable SplitMix64, independent of Python's randomized hash and row order.
    x = (int(query_id)+0x9e3779b97f4a7c15) & ((1 << 64)-1)
    x = ((x ^ (x >> 30))*0xbf58476d1ce4e5b9) & ((1 << 64)-1)
    x = ((x ^ (x >> 27))*0x94d049bb133111eb) & ((1 << 64)-1)
    return ((x ^ (x >> 31)) % modulus) == 0


def pressure(load):
    value = load.get('worker_pressure_ms')
    return value if value is not None and math.isfinite(value) and value >= 0 else None


@dataclass
class SourceHeat:
    epoch: str
    clock: float = 0.
    run: float = 0.
    cold: float = 0.
    entries: frozenset = frozenset()
    episodes: deque = field(default_factory=deque)
    cooldown: float = 0.
    unknown: int = 0


class OnlinePolicy:
    def __init__(self, settings):
        self.c = settings
        self.states = {}
        self.last_tick = None

    def tick(self, now, loads, hot_entries, hot_sources, healthy=True):
        before, self.last_tick = self.last_tick, now
        if before is None:
            return
        dt = now-before
        for source, load in loads.items():
            epoch = load.get('epoch')
            if source not in self.states or self.states[source].epoch != epoch:
                self.states[source] = SourceHeat(epoch)
            h = self.states[source]
            if not healthy or pressure(load) is None or not 0 < dt <= self.c['max_observation_gap_seconds']:
                h.unknown += 1
                continue
            h.clock += dt
            entries = frozenset(hot_entries.get(source, ()))
            overlap = len(entries & h.entries)/max(1, len(entries | h.entries))
            if source in hot_sources and entries:
                if h.entries and overlap < self.c['entry_stability_min']:
                    # A different hotspot is not another cycle of the old one.
                    h.run = 0.
                    h.episodes.clear()
                if h.run == 0:
                    h.entries = entries
                h.run += dt
                h.cold = 0.
            else:
                h.cold += dt
                if h.cold >= self.c['episode_cool_seconds']:
                    if h.run >= self.c['min_episode_seconds']:
                        h.episodes.append((h.clock, h.run))
                    h.run = 0.
            while h.episodes and h.clock-h.episodes[0][0] > self.c['recurrence_window_seconds']:
                h.episodes.popleft()
            # Bound recurrence memory even with pathological alternating input.
            while len(h.episodes) > self.c['recurrence_count']:
                h.episodes.popleft()

    def large_reason(self, source, now):
        h = self.states.get(source)
        if h is None or h.run < self.c['min_episode_seconds']:
            return 'insufficient_hot_duration'
        if now < h.cooldown:
            return 'large_cooldown'
        if h.run >= self.c['large_sustained_seconds']:
            return None
        if len(h.episodes)+1 >= self.c['recurrence_count']:
            return None
        return 'not_persistent_or_recurring'

    def attempted(self, source, now):
        if source in self.states:
            self.states[source].cooldown = now+self.c['large_cooldown_seconds']

    def snapshot(self):
        return {str(k):dict(healthy_seconds=h.clock, sustained_seconds=h.run,
                           episodes=len(h.episodes), unknown_windows=h.unknown,
                           stable_entries=len(h.entries), cooldown_until=h.cooldown)
                for k,h in self.states.items()}
