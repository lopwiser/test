from copy import deepcopy

import pytest

from payment_service.api.schemas import PaymentCreate
from payment_service.services.payments import request_fingerprint


def fingerprint(body):
    return request_fingerprint(PaymentCreate.model_validate(body).to_new_payment())


def test_stored_fingerprints_stay_valid(payment_body):
    metadata = {"rate": 1.5, "count": 2, "ok": True, "none": None, "nested": {"a": [1, "б"]}}
    assert fingerprint({**payment_body, "metadata": metadata}) == (
        "17829c965ee8c71caadae3ba270642f224a7d423a0827e4789425aa2a8d13a02"
    )


def test_equivalent_amounts_and_json_order_have_same_fingerprint(payment_body):
    equivalent = deepcopy(payment_body)
    equivalent["amount"] = "1500"
    equivalent["metadata"] = {"nested": {"b": [True, None], "a": 1}, "order_id": "123"}
    assert fingerprint(payment_body) == fingerprint(equivalent)


def test_omitted_metadata_and_empty_object_are_equivalent(payment_body):
    omitted = {key: value for key, value in payment_body.items() if key != "metadata"}
    assert fingerprint(omitted) == fingerprint({**omitted, "metadata": {}})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "1500.01"),
        ("currency", "EUR"),
        ("description", "Different order"),
        ("metadata", {"order_id": "124"}),
        ("webhook_url", "https://another.example/webhook"),
    ],
)
def test_each_business_field_affects_fingerprint(payment_body, field, value):
    assert fingerprint(payment_body) != fingerprint({**payment_body, field: value})
