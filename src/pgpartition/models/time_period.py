from collections import defaultdict
from datetime import timedelta, datetime

import isoweek as isoweek
from dateutil.tz import tzutc

from src.pgpartition.helpers.util import get_days_in_year, get_days_in_month


class TimePeriod(object):
    """
    Represents a time period, with helper functions
    """
    def __init__(self, start, end, utc=True):
        self.utc = utc
        if self.utc:
            self.start = self.start.replace(tzinfo=tzutc())
            self.end = self.end.replace(tzinfo=tzutc())

    def __str__(self):
        return 'TimePeriod from {} to {}'.format(self.start, self.end)

    def __repr__(self):
        return '<{}>'.format(str(self))

    def __eq__(self, other):
        return self.start == other.start and self.end == other.end

    def __ne__(self, other):
        return self.start != other.start or self.end != other.end

    def __gt__(self, other):
        return self.start > other.start

    def __ge__(self, other):
        return self.start >= other.start

    def __le__(self, other):
        return self.start <= other.start

    def __lt__(self, other):
        return self.start < other.start

    def split_by_day(self, full_day=False):
        """
        Splits the time period in days
        :full_day bool: consider the end of the day as 23:59 if True,
        otherwise use the time self.end has
        :rtype: list[TimePeriod]
        :return: a list that contains time periods that when combined together
        they amount to the initial / current period of time
        """
        days = []

        start = self.start
        end = (start + timedelta(days=1)).replace(
                hour=00, minute=00, second=00, microsecond=00
            )
        if full_day:
            end = end-timedelta(seconds=1)

        while True:
            if end >= self.end or (full_day and (end.date() == self.end.date())):
                days.append(TimePeriod(start, self.end))
                return days
            days.append(TimePeriod(start, end))
            start = end
            if full_day:
                start = start + timedelta(seconds=1)
            end = (start + timedelta(days=1)).replace(
                hour=00, minute=00, second=00, microsecond=00
            )
            if full_day:
                end = end - timedelta(seconds=1)

    def split_by_year(self) -> list:
        """
        Splits current time period by year
        :return: a list of time periods, each one of them has a start and end
        in a different year,
         e.g. [ TimePeriod(01-01-2018, 31-12-2018),
                TimePeriod(01-01-2019, 31-01-2019)
            ]
        """
        if self.start.year == self.end.year:
            return [self]

        days_in_year = get_days_in_year(self.start.year)
        start = self.start
        end = start + timedelta(days=days_in_year - start.timetuple().tm_yday)
        sub_periods = []

        while end <= self.end:
            sub_periods.append(TimePeriod(start, end))
            days_in_year = get_days_in_year(start.year)
            start = end
            if start.year == self.end.year:
                end = self.end
            else:
                end = start + timedelta(days=days_in_year)

        return sub_periods

    def split_by_year_and_month(self, strict=False) -> dict:
        """
        Splits the current time period by year and by month.
        E.g.:
        {
            2017: {
                12: TimePeriod instance
            },
            2018: {
                1: TimePeriod instance
            }
        }
        :param bool strict: finish at the end of the month (False), or exactly
        at the end date
        :return:
        """
        days_in_end_month = get_days_in_month(self.end.year, self.end.month)
        start_m = self.start
        end_m = self.end
        if not strict:
            start_m = self.start.replace(day=1, hour=0, minute=0, second=0)
            end_m = self.end.replace(
                                    day=days_in_end_month,
                                    hour=23,
                                    minute=59,
                                    second=59
                                )

        if self.start.year == self.end.year:
            if self.start.month == self.end.month:
                if strict:
                    return {
                        self.start.year: {
                            self.start.month: TimePeriod(
                                start_m,
                                end_m
                            )
                        }
                    }
                return {
                    self.start.year: {self.start.month: self}
                }

        sub_periods = {}

        for year_tw in self.split_by_year():
            y = year_tw.start.year
            m = year_tw.start.month
            days_in_month = get_days_in_month(self.start.year,
                                              self.start.month)
            start = year_tw.start
            end = start.replace(
                day=days_in_month,
                hour=23,
                minute=59,
                second=59,
                microsecond=59
            )
            curr_period = {y: defaultdict(list)}
            for i in range(m, 13):
                curr_period[y][i].append(TimePeriod(start, end))
                start = datetime(y, i, 1, 0, 0, 0, 0).replace(tzinfo=tzutc())
                days_in_month = get_days_in_month(start.year, i)
                if i == self.end.month and strict:
                    end = self.end
                else:
                    end = start.replace(
                        day=days_in_month,
                        hour=23,
                        minute=59,
                        second=59,
                        microsecond=59
                    )
            sub_periods.update(curr_period)

        print(sub_periods)
        return sub_periods

    def split_by_year_and_week(self, strict=False) -> dict:
        """
        Splits the current time period by year and by week.
        E.g.:
        {
            2017: {
                52: TimePeriod instance
            },
            2018: {
                0: TimePeriod instance
            }
        }
        :param bool strict: finish at the end of the week (False), or exactly
        at the end date
        :return:
        """
        sub_periods = {}
        start_y, start_w, _ = self.start.isocalendar()
        end_y, end_w, _ = self.end.isocalendar()

        if start_y == end_y and start_w == end_y:
            if strict:
                return {
                    start_y: {start_w: self}
                }
            else:
                week = isoweek.Week(start_y, start_w)
                start_wd = datetime(*week.monday().timetuple()[:6]).replace(
                    hour=0, minute=0, second=0
                )
                end_wd = datetime(*week.sunday().timetuple()[:6]).replace(
                    hour=23, minute=59, second=59
                )
                return {
                    start_y: {start_w: TimePeriod(
                                start_wd,
                                end_wd
                            )}
                }

        for year_tw in self.split_by_year():
            start_range = 0
            y = year_tw.start.year
            curr_period = {y: defaultdict(list)}
            weeks_in_year = isoweek.Week.last_week_of_year(y).week

            if y == self.end.year and y == end_y:
                weeks_in_year = self.end.isocalendar()[1]

            if y == self.start.year and y == start_y:
                start_range = self.start.isocalendar()[1]

            for w in range(start_range, weeks_in_year + 1):
                week = isoweek.Week(y, w)
                if y == self.start.year and w == start_range and strict:
                    start = self.start
                else:
                    start = datetime(
                        *week.monday().timetuple()[:6]
                    ).replace(hour=0, minute=0, second=0)
                    if y == self.start.year and y == start_y and w == start_range:
                        start = start.replace(
                            hour=self.start.hour,
                            minute=self.start.minute,
                            second=self.start.second
                        )
                if y == self.end.year and w == end_w and strict:
                    end = self.end
                else:
                    end = datetime(
                        *week.sunday().timetuple()[:6]
                    ).replace(hour=23, minute=59, second=59)
                curr_period[y][w] = TimePeriod(start, end)
            sub_periods.update(curr_period)

            if end_y not in sub_periods:
                week = isoweek.Week(end_y, end_w)
                start = datetime(
                    *week.monday().timetuple()[:6]).replace(
                                hour=0, minute=0, second=0
                )
                end = self.end
                if not strict:
                    end = datetime(*week.sunday().timetuple()[:6]).replace(
                                hour=23, minute=59, second=59)
                sub_periods[end_y] = {end_w: TimePeriod( start, end)}

        return sub_periods
