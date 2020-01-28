import os


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


class TableTools(object):

    @staticmethod
    def get_temporal_check(partition_field, start, end, new=False,
                           condition='AND'):
        new_prefix = ''
        if new:
            new_prefix = 'NEW.'

        if start > end:
            condition = 'OR'

        return f'{new_prefix}{partition_field} >= ' \
            f'\'{start.strftime("%Y-%m-%d %H:%M:%S")}\' ' \
            f'{condition} {new_prefix}{partition_field} <= ' \
            f'\'{end.strftime("%Y-%m-%d %H:%M:%S")}\' '