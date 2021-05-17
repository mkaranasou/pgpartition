import abc
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
from pgpartition.helpers.enums import BoundsEnum, PartitionByEnum
from pgpartition.helpers.util import get_default_data_path, Boundaries


class GeneratorBase(object, metaclass=abc.ABCMeta):
    def __init__(
            self,
            template_path=get_default_data_path(),
            template_name='partitioning_template.jinja2'
    ):
        self.template_path = template_path
        self.template_name = template_name
        print('self.template_path, ', self.template_path)
        print('self.template_name, ', self.template_name)
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
            split_by=None,
            index_by=None,
            template_path=get_default_data_path(),
            template_name='partitioning_template.jinja2'
    ):
        super().__init__(
            template_path=template_path, template_name=template_name
        )
        self.parent_table = Table(parent_table)
        self.partition_field = partition_field
        self.index_by = index_by
        self.split_by = split_by
        self.partitions = []
        
    def partition(self):
        raise NotImplementedError()

    def to_dict(self):
        return self.__dict__
        
        
class PartitionCondition(object, metaclass=abc.ABCMeta):
    def __init__(
            self,
            partition_column: str,
            bounds_type: BoundsEnum,
            partitioned_by: PartitionByEnum
    ):
        self.partition_column = partition_column
        self.boundaries = Boundaries(bounds_type)
        self.partitioned_by = partitioned_by

    @abc.abstractmethod
    def get_check(self):
        pass


class PartitionedTable(object, metaclass=abc.ABCMeta):
    """
    Represents the partitioned table
    """
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

    @abc.abstractmethod
    def partition_prefix(self):
        pass


class Partition(object):
    """
    Repsresents a partition.
    """
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


class TableIndex(object):
	"""
	Represents a table index.
	"""
	def __init__(self, name, table_name, fields):
	    self.name = name
	    self.table_name = table_name
	    self.fields = fields
	    
	def __str__(self):
	    return f'{self.name} ON {self.table_name} ({", ".join(self.fields)})'
	    
	def create(self):
	    return f'CREATE INDEX IF NOT EXISTS {str(self)};'
	
	def drop(self):
	    return f'DROP INDEX {str(self)};'
	 
	def to_dict(self):
	    return {
            'create': self.create(),
            'drop': self.drop(),
        }
