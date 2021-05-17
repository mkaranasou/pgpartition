from pgpartition.helpers.util import Boundaries
from pgpartition.models.time_period import TimePeriod
from pgpartition.models.base import PartitionCondition
from pgpartition.helpers.enums import BoundsEnum, PartitionByEnum


class TemporalPartitionCondition(PartitionCondition):
    def __init__(
            self,
            partition_column:str,
            period: TimePeriod,
            bounds_type: BoundsEnum,
            partitioned_by: PartitionByEnum,
            strict=False
    ):
        super(TemporalPartitionCondition).__init__(
            partition_column, bounds_type, partitioned_by
        )
        self.period = period
        self.strict = strict

    def get_check(self):
        return f'{self.partition_column} {self.boundaries.min_boundary()} {self.period.start} and ' \
               f'{self.partition_column} {self.boundaries.max_boundary()} {self.period.end}'

    def get_bounds(self):
        """
        Return a dict with the date range for the partitions
        e.g.
        for the weekly partition:
         {
            2017: {
                52: TimePeriod instance
            },
            2018: {
                0: TimePeriod instance
            }
        }
        :return:
        :rtype: dict[int][TimePeriod]
        """
        if self.partitioned_by == PartitionByEnum.w:
            return self.period.split_by_year_and_week(self.strict)
        elif self.partitioned_by == PartitionByEnum.m:
            return self.period.split_by_year_and_month(self.strict)
        else:
            raise ValueError(
                f'Unknown split by option {str(self.partitioned_by)}')

    @staticmethod
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
