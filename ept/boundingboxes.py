import math
import operator
from typing import Union, Tuple, Iterator, Optional, Iterable

Number = Union[float, int]


class BoundingBox2D:
    """ Represent a 2D BoundingBox.

                xmin <- width -> xmax
            +-------------------------------> x
            |
            |   point_min
    ymin    |    +------------+
      ↑     |    |            |
    height  |    |            |
      ↓     |    |            |
    ymax    |    +------------+
            |               point_max
            |
            y

    """

    def __init__(self, xmin: Number, ymin: Number, xmax: Number, ymax: Number) -> None:
        if xmin > xmax:
            raise ValueError("xmin ({}) cannot be superior to xmax ({})".format(xmin, xmax))

        if ymin > ymax:
            raise ValueError("ymin ({}) cannot be superior to ymax ({})".format(ymin, ymax))

        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

    @property
    def width(self) -> Number:
        return self.xmax - self.xmin

    @property
    def height(self) -> Number:
        return self.ymax - self.ymin

    @property
    def shape(self) -> Tuple[Number, Number]:
        return self.width, self.height

    @property
    def point_min(self) -> Tuple[Number, Number]:
        return self.xmin, self.ymin

    @property
    def point_max(self) -> Tuple[Number, Number]:
        return self.xmax, self.ymax

    @property
    def points(self) -> Tuple[Tuple, Tuple, Tuple, Tuple]:
        return (self.xmin, self.ymin), (self.xmax, self.ymin), (self.xmin, self.ymax), (self.xmax, self.ymax)

    @property
    def area(self) -> Number:
        return self.width * self.height

    @property
    def center(self) -> Tuple[Number, Number]:
        return (self.xmax + self.xmin) / 2, (self.ymax + self.ymin) / 2

    @property
    def is_null(self):
        return math.isclose(self.width, 0.0) or math.isclose(self.height, 0.0)

    def grow(self, coords: Iterable[Number]) -> None:
        coords = tuple(coords)
        try:
            xmin, ymin, xmax, ymax = coords
        except ValueError:
            xmin, ymin = xmax, ymax = coords

        self.xmin = min(self.xmin, xmin)
        self.ymin = min(self.ymin, ymin)

        self.xmax = max(self.xmax, xmax)
        self.ymax = max(self.ymax, ymax)

    def subtract(self, x: Number, y: Number):
        """ 'Moves' the bounding box by substracting its coordinates by (x, y)
        """
        self.xmin -= x
        self.ymin -= y
        self.xmax -= x
        self.ymax -= y
        return self

    def astype(self, newtype) -> 'BoundingBox2D':
        """ Returns a new BoundingBox with coordinates casted as the 'newtype'

        Parameters
        ----------
        newtype: the new type the BoundingBox Coordinates should be (e.g: int, float)
        """
        return BoundingBox2D(newtype(self.xmin), newtype(self.ymin), newtype(self.xmax), newtype(self.ymax))

    def intersection(self, other: 'BoundingBox2D') -> Optional['BoundingBox2D']:
        """ Compute the intersection between self and other

        Parameters
        ----------
        other: The other BoundingBox

        Returns
        -------
            None if the boxes do not overlap, or,
            A new BoundingBox representing the overlap between self and other.

        """
        top_left_point = max(self.xmin, other.xmin), max(self.ymin, other.ymin)
        bottom_right_point = min(self.xmax, other.xmax), min(self.ymax, other.ymax)

        bbox = None
        try:
            bbox = BoundingBox2D.from_points(top_left_point, bottom_right_point)
            if bbox.is_null:
                bbox = None
        except ValueError:
            pass
        finally:
            return bbox

    def intersect_over_union(self, other: 'BoundingBox2D') -> float:
        """ Returns the computed 'Intersect over Union' between self and other box.
        """
        intersection = self.intersection(other)
        if intersection is None:
            return 0.0
        return intersection.area / (self.area + other.area - intersection.area)

    def overlaps(self, other: 'BoundingBox2D') -> bool:
        x_overlap = self.xmin <= other.xmax and self.xmax >= other.xmin
        y_overlap = self.ymin <= other.ymax and self.ymax >= other.ymin
        return x_overlap and y_overlap

    def __contains__(self, item: 'BoundingBox2D') -> bool:
        """ Returns true if the bounding box pointed by 'item' is fully contained inside self.
        """
        top_left_inside = item.xmin >= self.xmin and item.ymin >= self.ymin
        bottom_right_inside = item.xmax <= self.xmax and item.ymax <= self.ymax
        return top_left_inside and bottom_right_inside

    def __iter__(self) -> Iterator[Number]:
        """ Returns an iterator over coordinates
        """
        return (getattr(self, p) for p in ['xmin', 'ymin', 'xmax', 'ymax'])

    def __hash__(self) -> int:
        return hash((self.xmin, self.ymin, self.xmax, self.ymax))

    def __eq__(self, other) -> bool:
        return all([self.xmin == other.xmin, self.ymin == other.ymin, self.xmax == other.xmax, self.ymax == other.ymax])

    def __repr__(self) -> str:
        return "<BoundingBox(xmin: {}, ymin: {}, xmax: {}, ymax: {})>".format(self.xmin, self.ymin, self.xmax,
                                                                              self.ymax)

    @classmethod
    def from_points(cls, point_min: Tuple[Number, Number], point_max: Tuple[Number, Number]) -> 'BoundingBox2D':
        return cls(point_min[0], point_min[1], point_max[0], point_max[1])

    @classmethod
    def from_shape(cls, point_min: Tuple[Number, Number], width: Number, height: Number) -> 'BoundingBox2D':
        point_max = tuple(map(operator.add, point_min, (width, height)))
        return cls.from_points(point_min, point_max)


class BoundingBox3D(BoundingBox2D):
    def __init__(self, xmin, ymin, zmin, xmax, ymax, zmax):
        super().__init__(xmin, ymin, xmax, ymax)
        self.zmin = zmin
        self.zmax = zmax

    def overlaps(self, other: 'BoundingBox3D'):
        z_overlap = self.zmin <= other.zmax and self.zmax >= other.zmin
        return super().overlaps(other) and z_overlap

    def __iter__(self):
        return iter((self.xmin, self.ymin, self.zmin, self.xmax, self.ymax, self.zmax))


BoundingBox = Union[BoundingBox3D, BoundingBox2D]
