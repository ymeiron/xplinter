from xplinter import Record
import os
from .driver import Driver

class Csv_driver(Driver):
    def __init__(self, record: Record, directory: str, reset: bool = False):
        self.tables: dict = {}
        for entity_name, entity in record.entity_dict.items():
            if entity_name.startswith('*'):
                continue
            self.tables[entity_name] = [field.name for field in entity.field_list if not field.name.startswith('*')]
        for view_name, view in record.view_dict.items():
            self.tables[view_name] = view.columns[:]
        self._directory = directory
        self.is_compatible()
        self._record = record
    def open(self):
        for table_name, columns in self.tables.items():
            filename = os.path.join(self._directory, table_name + '.csv')
            if not os.path.exists(filename):
                with open(filename, 'w') as f:
                    f.write(','.join(columns) + '\n')
    def is_compatible(self) -> bool:
        if not os.path.isdir(self._directory):
            raise RuntimeError(f'Directory `{self._directory}` does not exist')
        for table_name, columns in self.tables.items():
            filename = os.path.join(self._directory, table_name + '.csv')
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    header = f.readline().strip().split(',')
                if header != columns:
                    raise RuntimeError(f'Header of file `{filename}` does not match columns of table `{table_name}`')
        return True
    def is_empty(self) -> bool:
        raise NotImplementedError
    def reset(self):
        if not os.path.isdir(self._directory): os.makedirs(self._directory)
        for table_name, columns in self.tables.items():
            filename = os.path.join(self._directory, table_name + '.csv')
            with open(filename, 'w') as f:
                f.write(','.join(columns) + '\n')
    def write(self):
        for entity_name, entity in self._record.entity_dict.items():
            if entity_name.startswith('*'):
                continue
            df = entity.to_dataframe()
            df.to_csv(os.path.join(self._directory, entity_name + '.csv'), mode='a', index=False, header=False)
        for view_name, view in self._record.view_dict.items():
            df = view.to_dataframe()
            df.to_csv(os.path.join(self._directory, view_name + '.csv'), mode='a', index=False, header=False)
    def close(self):
        pass # Nothing to do for CSV writer