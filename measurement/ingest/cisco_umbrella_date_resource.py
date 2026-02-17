"""
A Cisco Umbrella resource. Takes a single date and maps to a resource string.
"""

from typing import Self

import arrow

from .base_date_resource import BaseDateResource


class CiscoUmbrellaDateResource(BaseDateResource):
    def __init__(self, date: arrow.Arrow, url: str):
        self._date = date
        self._url = url

    @classmethod
    def of_date(cls, date: arrow.Arrow) -> Self:
        date_str = date.format("YYYY-MM-DD")
        return cls(date, f"http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-{date_str}.csv.zip")

    def url(self) -> str:
        return self._url

    def date(self) -> arrow.Arrow:
        return self._date
