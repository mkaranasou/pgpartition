import abc
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
from pgpartition.helpers.util import get_default_data_path


class GeneratorBase(object, metaclass=abc.ABCMeta):
    def __init__(
            self,
            template_path=get_default_data_path(),
            template_name='data_partitioning.jinja2'
    ):
        self.template_path = template_path
        self.template_name = template_name
        self.j2_env = Environment(
            loader=FileSystemLoader(self.template_path),
            trim_blocks=True
        )
        self.template = self.j2_env.get_template(self.template_name)

    def __str__(self):
        self.template.globals['now'] = datetime.utcnow
        return self.template.render(self.to_dict())

    @abc.abstractmethod
    def to_dict(self):
        pass


class BasePartitioner(GeneratorBase):
    """
    Base partitioner, generates the data that the partitioning template needs
    """

    def __init__(
            self,
            parent_table,
            partition_field,
            index_by=None,
            template_path=get_default_data_path(),
            template_name='data_partitioning.jinja2'
    ):
        super().__init__(
            template_path=template_path, template_name=template_name
        )
        self.parent_table = parent_table
        self.partition_field = partition_field
        self.index_by = index_by
        self.partitions = []

    def to_dict(self):
        return self.__dict__


class PartitionedTable(object):
    def __init__(
            self,
            name,
            partition_field,
            partitioned_by,
            index_by,
            create_catch_all=True
    ):
        self.name = name
        self.partition_field = partition_field
        self.partitioned_by = partitioned_by
        self.index_by = index_by
        self.create_catch_all = create_catch_all
        self.catch_all_partition_name = f'{self.name}_catch_all'
        self.partitions = []


class Partition(object):
    def __init__(
            self,
            name,
            partition_field,
            index_by=None,
            is_catch_all=False
    ):
        self.name = name
        self.partition_field = partition_field
        self.index_by = index_by
        self.is_catch_all = is_catch_all
