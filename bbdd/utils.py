"""Utility helpers used across database modules."""

from conf import *
from typing import Callable, Optional, TypeVar


def divide(a: float, b: float) -> Optional[float]:
    """Safely divide ``a`` by ``b``.

    Parameters
    ----------
    a:
        Numerator of the division.
    b:
        Denominator of the division.

    Returns
    -------
    Optional[float]
        ``a / b`` or ``None`` when ``b`` is zero.
    """
    return a / b if b != 0 else None


F = TypeVar("F", bound=Callable[..., Optional[float]])


def capture_db_errors(func: F) -> F:
    """Decorator that logs exceptions raised by database operations.

    Parameters
    ----------
    func:
        The function performing a database action.

    Returns
    -------
    Callable
        Wrapped function that returns ``None`` when an exception occurs.
    """

    def error(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.warning(f"An exception occurred: {e}")
            logging.warning(traceback.format_exc())
            return None

    return error  # type: ignore[return-value]
