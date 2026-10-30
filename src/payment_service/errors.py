class IdempotencyConflict(Exception):
    pass


class PaymentNotFound(Exception):
    pass


class GatewayError(Exception):
    pass


class DatabaseUnavailable(Exception):
    pass


class ProcessingOwnershipLost(Exception):
    pass


def describe(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"
