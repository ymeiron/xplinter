from xplinter import Record, Entity, View
from xplinter.driver import Driver
import os

try:
    import pandas as pd
except ModuleNotFoundError:
    raise ImportError('CSV driver depends on Pandas') from None

def entity_to_df(entity: Entity, expose_hidden_fields: bool = False):
    columns = [field.name for field in entity.field_list]
    df = pd.DataFrame(entity.data, columns=columns)
    if not expose_hidden_fields:
        for column in columns:
            if column.startswith('*'):
                del df[column]
    return df

def view_to_df(view: View):
    df = pd.DataFrame(view.data, columns=view.columns)
    return df

class Csv_driver(Driver):
    def __init__(self, directory: str, reset: bool = False):
        self.tables: dict = {}
        self._directory = directory
        self._open: bool = False
        self._reset = reset
    def set_record(self, record: Record):
        self.tables: dict = {}
        for entity_name, entity in record.entity_dict.items():
            if entity_name.startswith('*'):
                continue
            self.tables[entity_name] = [field.name for field in entity.field_list if not field.name.startswith('*')]
        for view_name, view in record.view_dict.items():
            self.tables[view_name] = view.columns[:]
        self._record: Record = record
    def open(self):
        if not hasattr(self, '_record'): raise RuntimeError('CSV driver cannot open unless record is set')
        if self._open: raise RuntimeError('CSV driver already open')
        if self._reset:
            self.reset()
        else:
            self.is_compatible()
        for table_name, columns in self.tables.items():
            filename = os.path.join(self._directory, table_name + '.csv')
            if not os.path.exists(filename):
                with open(filename, 'w') as f:
                    f.write(','.join(columns) + '\n')
        self._open = True
    def is_compatible(self) -> bool:
        if not self._record: raise RuntimeError('CSV driver cannot check compatibility unless record is set')
        if not os.path.isdir(self._directory):
            raise RuntimeError(f'Directory `{self._directory}` does not exist')
        for table_name, columns in self.tables.items():
            filename = os.path.join(self._directory, table_name + '.csv')
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    header = f.readline().strip().split(',')
                if header != columns:
                    raise RuntimeError(f'Header of file `{filename}` does not match columns of table `{table_name}`')
        return True # We don't actually use the return value. Method raises if there is a compatibility issue
    def is_empty(self) -> bool:
        raise NotImplementedError
    def reset(self):
        os.makedirs(self._directory, exist_ok=True)
        for table_name, columns in self.tables.items():
            filename = os.path.join(self._directory, table_name + '.csv')
            with open(filename, 'w') as f:
                f.write(','.join(columns) + '\n')
    def write(self):
        if not self._open: raise RuntimeError('CSV driver `write` attempted while driver not open')
        for entity_name, entity in self._record.entity_dict.items():
            if entity_name.startswith('*'):
                continue
            df = entity_to_df(entity)
            df.to_csv(os.path.join(self._directory, entity_name + '.csv'), mode='a', index=False, header=False)
        for view_name, view in self._record.view_dict.items():
            df = view_to_df(view)
            df.to_csv(os.path.join(self._directory, view_name + '.csv'), mode='a', index=False, header=False)
    def flush(self):
        pass # no need to flush in this driver
    def close(self):
        if not self._open: raise RuntimeError('CSV driver `close` attempted while driver not open')
        self._open = False # Nothing else to do for CSV writer