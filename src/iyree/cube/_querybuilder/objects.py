"""Cube, Measure, Dimension, and Segment reference objects."""

from __future__ import annotations


class CubeObject:
    """Base class for objects defined in a Cube.js cube.

    An instance represents a *reference* to an object in a Cube.js schema.
    """

    def __init__(self, cube: Cube, name: str) -> None:
        self._cube = cube
        self.name = name

    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, self.__class__)
            and o._cube == self._cube
            and o.name == self.name
        )

    def __hash__(self) -> int:
        return hash((self.__class__, self._cube.name, self.name))

    def __str__(self) -> str:
        return f"{self._cube.name}.{self.name}"

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self._cube.name}", "{self.name}")'

    def serialize(self) -> str:
        """Serialize to the ``cube_name.object_name`` string format."""
        return str(self)


class Measure(CubeObject):
    """Reference to a measure defined in a cube."""


class Dimension(CubeObject):
    """Reference to a dimension defined in a cube."""


class Segment(CubeObject):
    """Reference to a segment defined in a cube."""


class Cube:
    """Factory that aliases object references with the correct cube name.

    Args:
        name: The cube name as defined in the Cube.js schema.

    Example::

        orders = Cube("orders")
        orders.measure("count")   # => Measure referencing "orders.count"
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and o.name == self.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name="{self.name}")'

    def __str__(self) -> str:
        return self.name

    def measure(self, name: str) -> Measure:
        """Create a :class:`Measure` reference in this cube."""
        return Measure(self, name)

    def dimension(self, name: str) -> Dimension:
        """Create a :class:`Dimension` reference in this cube."""
        return Dimension(self, name)

    def segment(self, name: str) -> Segment:
        """Create a :class:`Segment` reference in this cube."""
        return Segment(self, name)
