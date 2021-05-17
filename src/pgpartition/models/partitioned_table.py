from pgpartition.helpers.enums import PartitionByEnum
from pgpartition.helpers.util import get_temporal_check
from pgpartition.models.base import PartitionedTable


class TemporalPartitionedTable(PartitionedTable):
    """
    Representation of a partitioned by date table.
    """
    def __init__(
            self, 
            name, 
            period,
            partition_field,
            partitioned_by=PartitionByEnum.week,
            index_by=None,
            create_catch_all=True, 
            strict=False
    ):
        super().__init__(
            name, partition_field, partitioned_by, index_by, create_catch_all
        )
        self.strict = strict
        self.period = period
        self._partition_prefix = self.get_partition_prefix()
        self.start_year = self.period.start.year

    @property
    def partition_prefix(self):
        """
        The prefix of the partitioned table e.g.
        :return:
        """
        return self._partition_prefix.replace(
            '%year', str(self.period.start.year)
        ).replace('%unit', '')

    @property
    def start(self):
        """
        Start date - min date of all partitions
        :return:
        """
        if self.partitions:
            #todo: catchall?
            return min(self.partitions).active_period.start
        return self.period.start

    @property
    def end(self):
        """
        End date for all partitions
        :return:
        """
        if self.partitions:
            p = max(self.partitions)
            if p.is_catch_all:
                p = self.partitions[-2]
            return p.active_period.end
        return self.period.end

    def partitions_bounds_check(self):
        return get_temporal_check(
            self.partition_field,
            self.start,
            self.end,
            new=True
        )

    @property
    def field_value(self) -> str:
        return f'cast(extract({self.partitioned_by} ' \
            f'from NEW.{self.partition_field}) AS TEXT)'

    def get_partition_prefix(self) -> str:
        """
        The table prefix, in the form of:
        table_to_be_partitioned_%unit
        where unit will be replaced by the partition type
        (year, month, week etc)
        :return:
        """
        return f'{self.name}_y%year_{str(self.partitioned_by)[0]}%unit'

    def get_partition_name(self, year, unit) -> str:
        return self._partition_prefix.replace(
            '%year', str(year)
        ).replace(
            '%unit', str(unit)
        )

    def get_partition_range(self) -> dict:
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

    def partition(self) -> list:
        """
        Partition the table by the partition range
        :return: A list of partitions to be created
        :rtype: list[TemporalPartition]
        """
        for y, units in self.get_partition_range().items():
            for i, tw in units.items():
                self.partitions.append(self.get_partition_for(i, tw, y))
        if self.create_catch_all:
            self.partitions.append(self.get_catch_all_partition())

        return sorted(self.partitions, key=lambda p: p.active_period)

    def get_partition_for(self, unit, tw, year):
        """
        Returns a TemporalPartition for the specific time period (tw) and year
        - given a unit: month | week
        :param str unit: m (for month) or w (for week)
        :param TimePeriod tw:
        :param int year:
        :return:
        :rtype: TemporalPartition
        """
        return TemporalPartition(
            self.get_partition_name(year, unit),
            tw,
            self.partition_field,
            self.index_by
        )

    def get_catch_all_partition(self):
        """
        Create a TemporalPartition that will serve as a catch all partition
        :return:
        :rtype: TemporalPartition
        """
        start = self.end + timedelta(seconds=1)
        end = self.start - timedelta(seconds=1)
        return TemporalPartition(
            self.catch_all_partition_name,
            TimePeriod(start, end),
            self.partition_field,
            self.index_by,
            is_catch_all=True
        )

    def to_dict(self):
        return {
            'name': self.name,
            'partition_prefix': self.partition_prefix,
            'partitions': [p.to_dict() for p in self.partitions],
            'catch_all_partition_name': self.catch_all_partition_name,
            'partitioned_by': str(self.partitioned_by),
            'partition_field': self.partition_field,
            'field_value': self.field_value,
            'self_check': self.self_check()
        }