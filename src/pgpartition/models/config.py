import os.path
from typing import T, List

import dateutil
from dateutil.tz import tzutc
from pgpartition.helpers.util import get_default_data_path
from pyaml_env import BaseConfig


class Config(BaseConfig):
    database: 'DatabaseConfig' = None
    partition: 'PartitionConfig' = None

    def validate(self):
        if self.database:
            self.database.validate()
        if self.partition:
            self.partition.validate()

        if self.errors:
            raise ValueError(',\n'.join(self.errors))
        self._is_validated = True
        return self


class DatabaseConfig(BaseConfig):
    name: str
    user: str
    password: str
    host: str
    port: int

    def validate(self):
        self._is_validated = True
        return self


class PartitionConfig(BaseConfig):
    exec: bool = False
    output_file: str = None
    parent_table: str = None
    partition_type: str = 'temporal'
    column: str = None
    details: T = None

    def validate(self):
        if not self.exec:
            self.exec = False
        if not self.output_file and not self.exec:
            self.output_file = os.path.join(
                get_default_data_path(),
                'temp.slq'
            )
            print(f'Output file is not set, outputting to:{self.output_file}')
        if not self.parent_table:
            self.errors.append('parent_table is not set.')
        if not self.partition_type:
            self.partition_type = 'temporal'
            print(f'Partition type set to:{self.partition_type}')
        if not self.column:
            self.errors.append('column is not set.')
        if not self.details:
            self.errors.append('details not set.')

        self._is_validated = True
        return self


class TemporalPartitionDetailsConfig(BaseConfig):
    since: str = None
    until: str = None
    strict: bool = False
    split_by: str = 'week'
    index_by: List[str] = None

    def validate(self):
        if isinstance(self.since, str):
            self.since = dateutil.parser.parse(self.since).replace(
                tzinfo=tzutc())
        if isinstance(self.until, str):
            self.until = dateutil.parser.parse(self.until).replace(
                tzinfo=tzutc())
        if not self.split_by:
            self.errors.append('details.split_by is not set.')

        self._is_validated = True
        return self
