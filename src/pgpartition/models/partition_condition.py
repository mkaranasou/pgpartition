from pgpartition.models.time_period import TimePeriod


class PartitionCondition(object):
    def __init__(self, time_period: TimePeriod):
        self.time_period = time_period
