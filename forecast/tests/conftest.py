"""Quiet a warning that pandas emits from inside Timedelta on this NumPy."""

import warnings

warnings.filterwarnings(
    "ignore",
    message=r"The 'generic' unit for NumPy timedelta is deprecated",
    category=DeprecationWarning,
)
