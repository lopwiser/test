import pytest

from payment_service.services.retry import retry_delay


@pytest.mark.parametrize(
    ("attempt", "base", "maximum", "expected"),
    [
        (1, 1, 60, 1),
        (2, 1, 60, 2),
        (3, 1, 60, 4),
        (6, 1, 60, 32),
        (7, 1, 60, 60),
        (1_000_000, 1, 60, 60),
        (1, 2, 60, 2),
        (2, 2, 60, 4),
        (1, 10, 2, 2),
        (3, 0.25, 1, 1),
    ],
)
def test_capped_exponential_backoff(attempt, base, maximum, expected):
    assert retry_delay(attempt, base, maximum) == expected
