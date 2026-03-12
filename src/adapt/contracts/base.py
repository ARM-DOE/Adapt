"""Base contract enforcement utilities.

The require() function is the single enforcement mechanism for all contracts.
"""


class ContractViolation(RuntimeError):
    """Raised when a pipeline contract is violated.

    This indicates a bug in pipeline logic, not bad user input or recoverable
    science edge cases. It means a pipeline stage did not produce the invariants
    it promised.

    Key distinction:
    - ValueError: User/config error (handled by Pydantic)
    - ContractViolation: Pipeline bug (programmer error)
    - Exception: Recoverable science issues (try/except in algorithms)
    """
    pass


def require(condition: bool, message: str) -> None:
    """Enforce a pipeline contract.

    This is called at stage boundaries to verify the preceding stage
    produced the guaranteed invariants. It is fail-fast: no recovery,
    no fallback, no silence.

    Parameters
    ----------
    condition : bool
        The invariant that must be true. If False, ContractViolation is raised.

    message : str
        Error message explaining the contract violation (for debugging).

    Raises
    ------
    ContractViolation
        If condition is False. This indicates a bug in pipeline logic.

    Examples
    --------
    >>> require("x" in ds.coords, "Grid contract: missing 'x' coordinate")
    >>> require(df.shape[0] > 0, "Analysis contract: at least one cell expected")
    """
    if not condition:
        raise ContractViolation(message)
