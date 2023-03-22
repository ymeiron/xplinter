from xplinter import Record, Data_type
from xplinter.driver import Driver

import psycopg2, struct, datetime
from typing import List, Tuple, Dict
from io import BytesIO

class Table:
    """Create a data structure that can be copied to a table in a PostgreSQL database.

    The PostgreSQL `COPY` command is used to import data into a table in bulk
    from a file, rather than inserting data row by row. This class is used to
    create a file object (the `buffer` attribute) corresponding to a table per
    the binary format specification.

    Attributes
    ----------
    buffer : BytesIO
        A byte buffer containing the table data.
    """
    epoch = datetime.date(2000, 1, 1)
    def __init__(self, fields: List[Tuple[str,Data_type,int]]):
        """Construct a Table object.
        
        Parameters
        ----------
        fields : List[Tuple[str,Data_type,int]]
            Each element in this list is a tuple, where the first element is the
            name of the field, the second is a single data type, and the third
            the size (in case of char or text fields).
        """

        self.header = b'\x50\x47\x43\x4F\x50\x59\x0A\xFF\x0D\x0A\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        self.buffer = BytesIO()
        self.buffer.write(self.header)
        self.fields = fields
        self.field_count_bytes = struct.pack('!h', len(fields))
        self.counter = 0

    def reset(self):
        del self.buffer # Do we really need to delete? Can't we reuse?
        self.buffer = BytesIO()
        self.buffer.write(self.header)
        self.counter = 0

    def add_row(self, datum: list):
        """Add one row to the table.

        Parameters
        ----------
        datum : list
        """

        self.buffer.write(self.field_count_bytes)
        for i, (field_name, data_type, _) in enumerate(self.fields):
            if field_name.startswith('*'):
               continue
            value = datum[i]
            if value is None:
                self.buffer.write(b'\xFF\xFF\xFF\xFF')
                continue
            if (data_type == Data_type.text) or (data_type == Data_type.char):
                value = value.encode('utf-8')
                string_size = len(value)
                self.buffer.write(struct.pack('!i', string_size))
                self.buffer.write(value)
                continue
            if data_type == Data_type.xml:
                blob_size = len(value)
                self.buffer.write(struct.pack('!i', blob_size))
                self.buffer.write(value)
                continue
            if data_type == Data_type.date:
                date_int_repr = (value - Table.epoch).days
                self.buffer.write(struct.pack('!ii', 4, date_int_repr))
                continue
            field_type_code = Data_type.code(data_type)
            if field_type_code is None:
                raise NotImplemented
            field_size = struct.calcsize(field_type_code)
            self.buffer.write(struct.pack(f'!i{field_type_code}', field_size, value))
        self.counter += 1

    def finalize(self):
        """Finalize buffer by adding the file trailer word, and reset the current position."""
        self.buffer.write(b'\xFF\xFF')
        self.buffer.seek(0)


class Pgsql_driver(Driver):
    def __init__(self, db_config, reset: bool = False):
        self.db_config = db_config
        self.tables: Dict[str, List[Tuple[str, Data_type, int]]] = {}
        self._open: bool = False
        if reset:
            raise NotImplementedError
    def set_record(self, record: Record):
        self.table_buffers: Dict[str, Table] = {}
        for entity_name, entity in record.entity_dict.items():
            if entity_name.startswith('*'):
                continue
            self.tables[entity_name] = [(field.name, field.data_type, field.type_size) for field in entity.field_list if not field.name.startswith('*')]
            self.table_buffers[entity_name] = Table(self.tables[entity_name])
        for view_name, view in record.view_dict.items():
            if view.entity is None:
                raise RuntimeError
            self.tables[view_name] = []
            for col, idx in zip(view.columns, view.indices):
                field = view.entity.field_list[idx]
                self.tables[view_name].append((col, field.data_type, field.type_size))
            self.table_buffers[view_name] = Table(self.tables[view_name])
        self._record: Record = record
        
    def open(self):
        self.con = psycopg2.connect(**self.db_config)
        self.cur = self.con.cursor()
        self._open = True
    def is_compatible(self) -> bool:
        raise NotImplementedError
    def is_empty(self) -> bool:
        raise NotImplementedError
    def reset(self):
        if not self._open: raise RuntimeError('PostgreSQL driver `reset` attempted while driver not open')
        for table_name, field_list in self.tables.items():
            columns_string = ''
            for field_tuple in field_list:
                field_name, field_type, type_size = field_tuple
                type_string = field_type.name.upper()
                if ((field_type == Data_type.char) or (field_type == Data_type.text)) and (type_size > 0):
                    type_string += f'({type_size})'
                columns_string += field_name + ' ' + type_string + ', '
            columns_string = columns_string[:-2] # Get rid of last comma and space
            self.cur.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE')
            self.cur.execute(f'CREATE TABLE {table_name} ({columns_string})')
    def write(self):
        if not self._open: raise RuntimeError('PostgreSQL driver `write` attempted while driver not open')
        for entity_name, entity in self._record.entity_dict.items():
            if entity_name.startswith('*'):
                continue
            for _row in entity.data:
                row = [_row[i] for i in entity.visible_field_inidices]
                self.table_buffers[entity_name].add_row(row)
        for view_name, view in self._record.view_dict.items():
            for row in view.data:
                self.table_buffers[view_name].add_row(row)
    def flush(self):
        for table_name, table_buffer in self.table_buffers.items():
            table_buffer.finalize()
            self.cur.copy_expert(f'COPY {table_name} FROM STDIN WITH BINARY', table_buffer.buffer)
            table_buffer.reset()
        self.con.commit()
    def close(self):
        if not self._open: return
        self.flush()
        self.con.close()
        self._open = False