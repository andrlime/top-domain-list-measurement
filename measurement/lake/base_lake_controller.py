"""
An abstract lake manager.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Self


class BaseLakeController(ABC):
    @classmethod
    @abstractmethod
    def create(cls, path: Path, output: Path) -> Self: ...
