"""Gain/cost policy for data-node branches and the reference search baseline."""

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class Candidate:
    global_id: int
    estimated_distance: float
    uncertainty: float = 0.0


@dataclass(frozen=True)
class Action:
    key: str
    remote: bool
    candidates: tuple[Candidate, ...]
    cost_ms: float
    wait_ms: float = 0.0


@dataclass(frozen=True)
class Policy:
    kappa: float = 1.0
    exploration: float = 0.02
    max_wait_ms: float = 1.0

    def __post_init__(self):
        if (not all(math.isfinite(v) for v in (self.kappa, self.exploration, self.max_wait_ms))
                or self.kappa < 0 or self.exploration <= 0 or self.max_wait_ms <= 0):
            raise ValueError("Invalid scheduling policy")

    def score(self, action: Action, threshold: float, scale: float) -> float:
        if (not math.isfinite(threshold) or not math.isfinite(scale) or scale <= 0
                or not math.isfinite(action.cost_ms) or action.cost_ms <= 0
                or not math.isfinite(action.wait_ms) or action.wait_ms < 0
                or not action.candidates):
            raise ValueError("Action requires finite scale, positive cost and candidates")
        gains = {}
        for v in action.candidates:
            if (not isinstance(v.global_id, int) or not 0 <= v.global_id < 2**64
                    or not math.isfinite(v.estimated_distance)
                    or not math.isfinite(v.uncertainty) or v.uncertainty < 0):
                raise ValueError("Invalid scheduling candidate")
            lower_score = v.estimated_distance - self.kappa * v.uncertainty
            gain = max((threshold - lower_score) / scale, 0) + self.exploration
            gains[v.global_id] = max(gains.get(v.global_id, 0), gain)
        return sum(gains.values()) / action.cost_ms * (1 + action.wait_ms / self.max_wait_ms)

    def choose(self, actions, threshold: float, scale: float):
        """Only pass currently executable actions; oldest expired remote goes first."""
        actions = list(actions)
        if not actions:
            return None
        if len({a.key for a in actions}) != len(actions):
            raise ValueError("Action keys must be unique")
        scores = {a.key: self.score(a, threshold, scale) for a in actions}
        expired = [a for a in actions if a.remote and a.wait_ms >= self.max_wait_ms]
        if expired:
            return min(expired, key=lambda a: (-a.wait_ms, a.key))
        return min(actions, key=lambda a: (-scores[a.key], a.remote, a.key))


def remote_probability(remote_score, local_score, weight, temperature):
    """P = sigmoid((log(weight*U_remote)-log(U_local))/temperature)."""
    if any(not math.isfinite(v) for v in (remote_score,local_score,weight,temperature)):
        raise ValueError('Nonfinite probability input')
    if min(remote_score,local_score,temperature)<=0 or weight<0:
        raise ValueError('Invalid probability input')
    if weight==0: return 0.
    z=(math.log(weight)+math.log(remote_score)-math.log(local_score))/temperature
    if z>=0: return 1/(1+math.exp(-z))
    e=math.exp(z)
    return e/(1+e)
