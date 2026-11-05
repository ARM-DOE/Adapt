# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Scan history contract.

Validates the rolling-window list of prior scan contexts passed to
multi-scan modules (required_history > 1). Each entry is a context dict
produced by the processor for a completed scan.
"""

from adapt.contracts.pipeline import require


def check_scan_history(history: list) -> None:
    """Validate the scan_history context key for multi-scan modules.

    Each entry must be a dict containing at minimum ``segmented_ds``
    (output of the detection module) and ``scan_time``.

    Raises
    ------
    ContractViolation
        If the history is empty, not a list, or any entry is malformed.
    """
    require(isinstance(history, list), "scan_history must be a list")
    require(len(history) >= 1, "scan_history must be non-empty")
    for i, ctx in enumerate(history):
        require(isinstance(ctx, dict), f"scan_history[{i}] must be a dict")
        require("segmented_ds" in ctx, f"scan_history[{i}] missing 'segmented_ds'")
        require("scan_time" in ctx, f"scan_history[{i}] missing 'scan_time'")


def check_prior_scan(prior: dict | None) -> None:
    """Validate the prior_scan context key.

    ``None`` when no usable predecessor exists (first scan, or the time gap to
    the previous scan is too large); otherwise the previous completed scan's
    context dict, which must carry ``scan_time``.
    """
    if prior is None:
        return
    require(isinstance(prior, dict), "prior_scan must be a dict or None")
    require("scan_time" in prior, "prior_scan missing 'scan_time'")
