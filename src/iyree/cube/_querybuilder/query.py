"""Query builder class for constructing Cube.js queries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from .enums import Order
from .filters import Filter, TimeDimension
from .objects import Dimension, Measure, Segment


class Query:
    """Typed builder for Cube.js queries.

    Args:
        measures: Measures to compute.
        time_dimensions: Time dimensions for filtering / grouping.
        dimensions: Dimensions for grouping.
        filters: Data filters.
        segments: Segments to apply.
        limit: Maximum number of result rows.
        offset: Row offset for pagination.
        order: Ordering of result rows.
        timezone: IANA timezone name applied to time dimensions.
        ungrouped: If ``True``, skip ``GROUP BY`` in the generated SQL.
    """

    def __init__(
        self,
        measures: List[Measure],
        time_dimensions: Optional[List[TimeDimension]] = None,
        dimensions: Optional[List[Dimension]] = None,
        filters: Optional[List[Filter]] = None,
        segments: Optional[List[Segment]] = None,
        limit: int = 5_000,
        offset: int = 0,
        order: Optional[List[Tuple[Union[Dimension, Measure], Order]]] = None,
        timezone: str = "UTC",
        ungrouped: bool = False,
    ) -> None:
        self.measures = measures
        self.time_dimensions = time_dimensions
        self.dimensions = dimensions
        self.filters = filters
        self.segments = segments
        self.limit = limit
        self.offset = offset
        self.order = order
        self.timezone = timezone
        self.ungrouped = ungrouped

    def serialize(self) -> Dict[str, Any]:
        """Serialize the query into the dict format expected by Cube.js.

        Only non-default optional fields are included to avoid Cube.js
        rejecting unexpected keys.
        """
        result: Dict[str, Any] = {
            "measures": [m.serialize() for m in self.measures],
        }
        if self.time_dimensions:
            result["timeDimensions"] = [
                d.serialize() for d in self.time_dimensions
            ]
        if self.dimensions:
            result["dimensions"] = [d.serialize() for d in self.dimensions]
        if self.filters:
            result["filters"] = [f.serialize() for f in self.filters]
        if self.segments:
            result["segments"] = [s.serialize() for s in self.segments]
        if self.limit != 5_000:
            result["limit"] = self.limit
        if self.offset != 0:
            result["offset"] = self.offset
        if self.order:
            result["order"] = [
                (obj.serialize(), order.value) for obj, order in self.order
            ]
        if self.timezone != "UTC":
            result["timezone"] = self.timezone
        if self.ungrouped:
            result["ungrouped"] = self.ungrouped
        return result
