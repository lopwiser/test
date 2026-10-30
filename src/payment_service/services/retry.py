from math import ceil, log2


def retry_delay(attempt: int, base_delay: float, max_delay: float) -> float:
    exponent = min(attempt - 1, ceil(log2(max_delay / base_delay)))
    return min(base_delay * 2**exponent, max_delay)
