import operator
import os

from pgpartition.helpers.enums import BoundsEnum


def get_default_data_path():
    """
    Returns the absolute path to the data folder
    :return:
    """
    return f'{os.path.dirname(os.path.realpath(__file__))}/../../../data'


def get_days_in_year(year):
    """
    Returns the number of days in a specific year
    :param int year: the number of the year
    :return: the number of days in the specific year
    :rtype: int
    """
    from calendar import Calendar
    days_in_year = 0
    for m in Calendar().yeardays2calendar(year, width=12)[0]:
        for w in m:
            days_in_year += len([pair for pair in w if pair[0] != 0])
            # len(list(filter((0).__ne__, w)))

    return days_in_year


def get_days_in_month(year, month):
    """
    Returns the number of days in the month of a specific year
    :param int year: the year of the month
    :param int month: the number of the month
    :return:
    :rtype: int
    """
    from calendar import monthrange
    return monthrange(year, month)[1]


def get_temporal_check(
        partition_field, start, end, new=False, condition='AND'
):
    new_prefix = ''
    if new:
        new_prefix = 'NEW.'

    if start > end:
        condition = 'OR'

    return f'{new_prefix}{partition_field} >= ' \
        f'\'{start.strftime("%Y-%m-%d %H:%M:%S")}\' ' \
        f'{condition} {new_prefix}{partition_field} <= ' \
        f'\'{end.strftime("%Y-%m-%d %H:%M:%S")}\' '


class Boundaries(object):
    """

    """
    bound_type_to_bounds = {
        BoundsEnum.open: (operator.gt, operator.lt),
        BoundsEnum.closed: (operator.ge, operator.le),
        BoundsEnum.closed_left: (operator.ge, operator.lt),
        BoundsEnum.closed_right: (operator.gt, operator.le),
    }
    bound_type_to_bounds_str = {
        BoundsEnum.open: (">", "<"),
        BoundsEnum.closed: (">=", "<="),
        BoundsEnum.closed_left: (">=", "<"),
        BoundsEnum.closed_right: (">", "<="),
    }

    def __init__(self, bounds_type: BoundsEnum):
        self.bounds_type = bounds_type
        self.bounds = self.bound_type_to_bounds[self.bounds_type]
        self.bounds_str = self.bound_type_to_bounds_str[self.bounds_type]

    def min_boundary(self):
        return self.bounds[0]

    def max_boundary(self):
        return self.bounds[1]

    def str_min_boundary(self):
        return self.bounds_str[0]

    def str_max_boundary(self):
        return self.bounds_str[1]


TEMPLATE_FOLDER = os.path.join(get_default_data_path())
CONF_FOLDER = os.path.join(get_default_data_path(), '..', 'conf')
