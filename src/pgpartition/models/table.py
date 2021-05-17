from pgpartition.models.partition_condition import TemporalPartitionCondition


class Table(object):
    def __init__(
            self,
            name,
            parent: 'pgpartition.models.table.Table'=None,
            partition_condition: TemporalPartitionCondition=None
    ):
        self.name = name
        self.parent = parent
        self.partition_condition = partition_condition
        self.partitions = []

    def partition(self) -> None:
        for name, period in self.partition_condition.get_partition_periods():
            self.partitions.append(Table(name, self))

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'partitions': [p.to_dict() for p in self.partitions],
        }