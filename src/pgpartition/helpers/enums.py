from enum import Enum


class BaseStrEnum(Enum):

    def __str__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        elif isinstance(other, self.__class__):
            return self.value == other.value
        return False

    def __ne__(self, other):
        if isinstance(other, str):
            return self.value != other
        elif isinstance(other, self.__class__):
            return self.value != other.value
        return True

    def __hash__(self):
        return hash(self.value)


class PartitionByEnum(BaseStrEnum):
    week = 'week'
    month = 'month'


class BoundsEnum(Enum):
    closed = 0
    closed_left = 1
    closed_right = 2
    open = 3
    equality = -1
