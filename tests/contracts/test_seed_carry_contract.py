# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Contracts for the detection seed-carry state and the prior-scan context."""

from datetime import UTC, datetime

import pytest

from adapt.contracts import ContractViolation, check_prior_scan, check_seed_carry

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("ages", [(), (0,), (0, 1, 2)])
def test_seed_carry_accepts_tuples_of_non_negative_ints(ages):
    check_seed_carry(ages)


@pytest.mark.parametrize("bad", [[0, 1], (0.5,), (-1,), (True,), None])
def test_seed_carry_rejects_non_tuple_or_non_int_or_negative(bad):
    with pytest.raises(ContractViolation):
        check_seed_carry(bad)


def test_prior_scan_accepts_none():
    check_prior_scan(None)


def test_prior_scan_accepts_a_scan_context():
    check_prior_scan({"scan_time": datetime(2024, 1, 1, tzinfo=UTC)})


def test_prior_scan_rejects_a_context_without_scan_time():
    with pytest.raises(ContractViolation):
        check_prior_scan({})
