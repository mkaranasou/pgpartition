from pgpartition.helpers.util import get_default_data_path
from pgpartition.models.base import BasePartitioner
from pgpartition.models.config import Config
from pgpartition.models.partitioned_table import TemporalPartitionedTable
from pgpartition.models.time_period import TimePeriod


class TemporalPartitioner(BasePartitioner):
    """
    Partition data based on a date field.
    """
    def __init__(
            self,
            config: Config,
            template_path=get_default_data_path(),
            template_name='partitioning_template.jinja2'
    ):
        """
        Partitions the parent table by a time period, e.g. w for per week
        partitions, m for per month partitions.

        :param str partitioned_table_name: the name of the parent table
        :param str partition_field: the name of the field to partition by
        :param baskerville.db.base.TimePeriod time_window: the period to
        partition data for, e.g. a year, starting from 1-1-2018 to 31-12-2018
        :param str split_by: w for week, m for month
        :param index_by: list of fields to index the partitions by
        """
        super().__init__(
            config.partition.parent_table,
            config.partition.column,
            split_by=config.partition.details.split_by,
            index_by=config.partition.details.index_by,
            template_path=template_path,
            template_name=template_name,
        )
        self.config = config
        self.time_period = TimePeriod(
            self.config.partition.details.since,
            self.config.partition.details.until,
        )
        self.strict = config.partition.details.strict
        self.partitioned_table = TemporalPartitionedTable(
            config.partition.parent_table,
            self.time_period,
            config.partition.column,
            partitioned_by=self.split_by,
            index_by=self.index_by,
            strict=self.strict
        )
        # self.partitions = self.partitioned_table.partition()

    def to_dict(self):
        return self.partitioned_table.to_dict()
        
    def partition(self):
        self.partitions = self.parent_table.partition()
        return self.to_dict()
