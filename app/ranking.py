import math
import time
from typing import Optional

from .config import RANKING_DECAY_PER_HOUR


def current_time_ms() -> int:
    return int(time.time() * 1000)


def compute_decay_hours(last_clicked_at_ms: Optional[int], now_ms: Optional[int] = None) -> float:
    now_ms = now_ms or current_time_ms()
    if not last_clicked_at_ms:
        return 0.0
    return max(0.0, (now_ms - last_clicked_at_ms) / 3_600_000)


def compute_ranking_score(
    clicks_total: int,
    recent_clicks: float,
    last_clicked_at_ms: Optional[int],
    now_ms: Optional[int] = None,
    decay_per_hour: float = RANKING_DECAY_PER_HOUR,
) -> float:
    now_ms = now_ms or current_time_ms()
    decay_hours = compute_decay_hours(last_clicked_at_ms, now_ms)
    decay = decay_hours * decay_per_hour
    return math.log(clicks_total + 1) + (recent_clicks * 0.7) - decay
