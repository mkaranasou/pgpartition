def execute_partitioning() -> None:
    Session.execute(text(get_temporal_partitions(maintenance_conf)))
    print('Partitioning done...')