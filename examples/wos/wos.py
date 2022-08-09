from xplinter.record_generator import Generate_record
from xplinter import Cardinality, Record
from lxml import etree
import os

output_data_dir = 'output_data'
input_data_dir  = 'input_data'
input_filename  = 'wos_long_sample.xml'

class Driver:
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
        for entity_name, entity in record.entity_dict.items():
            if entity_name.startswith('*'):
                continue
            df = entity.to_dataframe()
            df.to_csv(os.path.join(self._directory, entity_name + '.csv'), mode='a', index=False, header=False)
        for view_name, view in record.view_dict.items():
            df = view.to_dataframe()
            df.to_csv(os.path.join(self._directory, view_name + '.csv'), mode='a', index=False, header=False)
    def close(self):
        pass # Nothing to do for CSV writer

def postprocess(record: Record) -> None:
    author_address = record.entity_dict['author_address']
    author         = record.entity_dict['author']
    address        = record.entity_dict['address']
    for row in author.data:
        try:
            author_id = row[0]
            addr_no_list = row[7].split()
            for addr_no in addr_no_list:
                address_id = address.data[int(addr_no) - 1][0]
                author_address.data.append([author_id, address_id ])
        except:
            pass

# Loading the Xplinter record description parser
generate_record = Generate_record()

# Generating the record parser
with open('wos.xplinter', 'r') as f:
    content = f.read()
record = generate_record(content)

# Obtaining the XML tree
with open(os.path.join(input_data_dir, input_filename), 'rb') as f:
    tree = etree.fromstring(f.read(), None)
    #tree = [tree.getchildren()[0]]

# Clean up output directory:
driver = Driver(record, output_data_dir)
driver.open()
driver.reset()

for node in tree:
    record.process(node)
    postprocess(record)
    driver.write()
    record.reset()
driver.close()