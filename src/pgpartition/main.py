from datetime import datetime

from dateutil import parser as p
from jinja2 import Environment, FileSystemLoader
from pgpartition.helpers.util import TEMPLATE_FOLDER
from pgpartition.models.config import Config, PartitionConfig, \
    TemporalPartitionDetailsConfig
from pgpartition.models.partitioner import TemporalPartitioner
from pyaml_env import parse_config

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        help='The path to the yaml config file: optional',
        type=p.parse
    )
    parser.add_argument(
        '-p', '--parent_table', help='The name of the table to be partitioned'
    )
    parser.add_argument('-co', '--column', help='The table column to partition by')
    parser.add_argument(
        '-t', '--partition_type', help='The type of data partitioner, e.g. temporal'
    )
    parser.add_argument(
        '-s', '--since', help='Optional', type=p.parse
    )
    parser.add_argument(
        '-u', '--until', help='The type of data partitioner',
        type=p.parse
    )
    parser.add_argument(
        '-b', '--split_by', help='Split the data by week or month'
    )
    parser.add_argument('-st', '--strict', help='tbd', default=False)
    parser.add_argument(
        '-i', '--index_by', help='Index the data', dest='indexby',
        type=lambda s: [str(item) for item in s.split(',')]
    )
    args = parser.parse_args()
    config = {}
    if args.config:
        config = Config(parse_config(args.config))

    if not config:
        p = 'Please either provide a value or a config file'
        if not args.parent_table:
            raise ValueError(f'No parent table name provided. {p}')
        if not args.column:
            raise ValueError(f'Column is not provided.{p}')
        if args.partition_type == 'temporal':
            if not args.since or not args.until or not args.splitby:
                raise ValueError(
                    'Temporal partitioner needs since and until and splitby'
                )
        config = Config({})
        config.partition = PartitionConfig({})
        config.partition.parent_table = args.parent_table
        config.partition.partition_type = args.partition_type
        config.partition.column = args.column
        config.partition.details = TemporalPartitionDetailsConfig({})
        config.partition.details.since = args.since
        config.partition.details.until = args.until
        config.partition.details.strict = args.strict
        config.partition.details.split_by = args.split_by
        config.partition.details.index_by = args.index_by \
            if hasattr(args, 'index_by') \
            else None

    config = config.validate()

    tp = TemporalPartitioner(
        config
    )
    tp.partition()

    print(tp.partitions)
    # # Create the jinja2 environment - trim_blocks helps control whitespace.
    # j2_env = Environment(loader=FileSystemLoader(TEMPLATE_FOLDER),
    #                      trim_blocks=True)
    # # get template and render
    # template = j2_env.get_template("partitioning_template.jinja2")
    # template.globals['now'] = datetime.utcnow
    # rendered_data = template.render(tp.to_dict())
    #
    # with open('test.sql', 'wb') as out_file:
    #     out_file.write(bytes(rendered_data.encode('utf-8')))