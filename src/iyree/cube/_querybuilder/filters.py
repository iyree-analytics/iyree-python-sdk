"""Filter, TimeDimension, DateRange, and boolean expression classes."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from .enums import FilterOperator, TimeGranularity
from .objects import Dimension, Measure


class DateRange:
    """Absolute or relative date range for time-dimension filtering.

    Provide *either* ``(start_date, end_date)`` for an absolute range or
    ``relative`` for a Cube.js relative date string (e.g. ``"last 7 days"``).

    Args:
        start_date: Start of the absolute range (``YYYY-MM-DD`` or ISO-8601).
        end_date: End of the absolute range.
        relative: Relative date expression understood by Cube.js.

    Raises:
        ValueError: If neither absolute nor relative range is provided.
    """

    def __init__(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        relative: Optional[str] = None,
    ) -> None:
        if not ((start_date is not None and end_date is not None) or relative is not None):
            raise ValueError(
                "Provide either (start_date, end_date) or relative"
            )
        self.start_date = start_date
        self.end_date = end_date
        self.relative = relative

    def serialize(self) -> Union[List[str], str]:
        """Serialize for inclusion in a Cube.js query."""
        if self.start_date is not None and self.end_date is not None:
            return [self.start_date, self.end_date]
        if self.relative:
            return self.relative
        raise ValueError("Expected (start_date, end_date) or relative")


class TimeDimension:
    """Combination of a time dimension reference, date range, and granularity.

    Args:
        dimension: Reference to a time-type dimension.
        date_range: Single date range for filtering.
        compare_date_range: List of date ranges for period-over-period comparison.
        granularity: Grouping granularity.

    Raises:
        ValueError: If neither *date_range* nor *compare_date_range* is provided.
    """

    def __init__(
        self,
        dimension: Dimension,
        date_range: Optional[DateRange] = None,
        compare_date_range: Optional[List[DateRange]] = None,
        granularity: TimeGranularity = TimeGranularity.null,
    ) -> None:
        if date_range is None and compare_date_range is None:
            raise ValueError(
                "Provide either date_range or compare_date_range"
            )
        self.dimension = dimension
        self.date_range = date_range
        self.compare_date_range = compare_date_range
        self.granularity = granularity

    def serialize(self) -> Dict[str, Any]:
        """Serialize for inclusion in a Cube.js query."""
        result: Dict[str, Any] = {
            "dimension": self.dimension.serialize(),
        }
        if self.granularity is not TimeGranularity.null:
            result["granularity"] = self.granularity.value
        if self.date_range:
            result["dateRange"] = self.date_range.serialize()
        if self.compare_date_range:
            result["compareDateRange"] = [
                dr.serialize() for dr in self.compare_date_range
            ]
        return result


class Filter:
    """A data filter applied to a measure or dimension.

    Args:
        member: The dimension or measure to filter on.
        operator: The filter operator.
        values: Filter values (stringified in serialization).
    """

    def __init__(
        self,
        member: Union[Dimension, Measure],
        operator: FilterOperator,
        values: Optional[List[Any]] = None,
    ) -> None:
        self.member = member
        self.operator = operator
        self.values = values

    def serialize(self) -> Dict[str, Any]:
        """Serialize for inclusion in a Cube.js query."""
        result: Dict[str, Any] = {
            "member": self.member.serialize(),
            "operator": self.operator.value,
        }
        if self.values is not None:
            result["values"] = [str(v) for v in self.values]
        return result


class BooleanExpression:
    """Base class for boolean filter expressions (``and`` / ``or``)."""

    operator: str

    def __init__(self, *operands: Union[BooleanExpression, Filter]) -> None:
        self.operands = operands

    def serialize(self) -> Dict[str, List[Any]]:
        """Serialize for inclusion in a Cube.js query."""
        return {self.operator: [o.serialize() for o in self.operands]}


class Or(BooleanExpression):
    """Boolean ``or`` filter expression."""

    operator = "or"


class And(BooleanExpression):
    """Boolean ``and`` filter expression."""

    operator = "and"
