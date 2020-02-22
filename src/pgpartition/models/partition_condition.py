from pgpartition.models.time_period import TimePeriod
from pgpartition.models.base import PartitionCondition


class TemporalPartitionCondition(PartitionCondition):
    def __init__(
    self, partition_column:str,  time_period: TimePeriod
    ):
        super(TemporalPartitionCondition).__init(partition_column)
        self.time_period = time_period
        
    def condition_check(self):
        return f'{self.partition_column} <= {self.period.start} and {self.partition_column} >= {self.period.end}'
        