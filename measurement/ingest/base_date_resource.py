"""
An abstract date resource. Takes a single date and maps to a resource string.
"""

from abc import ABC, abstractmethod
from typing import Self

import arrow


class BaseDateResource(ABC):
    @classmethod
    @abstractmethod
    def of_date(cls, date: arrow.Arrow) -> Self: ...
