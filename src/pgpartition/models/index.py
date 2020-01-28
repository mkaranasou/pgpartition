class TableIndex(object):
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
