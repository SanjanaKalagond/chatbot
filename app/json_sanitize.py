"""Recursively replace NaN/Inf and non-JSON-native values so responses serialize safely."""

from __future__ import annotations

import math
from typing import Any


def sanitize_for_json(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, int) and not isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    try:
        import numpy as np

        if isinstance(obj, np.generic):
            if isinstance(obj, np.floating):
                x = float(obj)
                return None if (math.isnan(x) or math.isinf(x)) else x
            if isinstance(obj, np.integer):
                return int(obj)
            return sanitize_for_json(obj.item())
        if isinstance(obj, np.ndarray):
            return sanitize_for_json(obj.tolist())
    except ImportError:
        pass

    try:
        import pandas as pd

        if obj is pd.NA:
            return None
    except ImportError:
        pass

    return obj
