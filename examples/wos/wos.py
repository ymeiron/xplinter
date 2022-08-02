from xplinter.record_generator import Generate_record
from xplinter import Cardinality, Record
from lxml import etree
import os

output_data_dir = 'output_data'
input_data_dir  = 'input_data'
input_filename  = 'wos_long_sample.xml'

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

# Clean up output directory:
for entity_name, entity in record.entity_dict.items():
    if entity_name.startswith('*'):
        continue
    filename = os.path.join(output_data_dir, entity_name + '.csv')
    columns = [field.name for field in entity.field_list if not field.name.startswith('*')]
    with open(filename, 'w') as f:
        f.write(','.join(columns) + '\n')
for view_name, view in record.view_dict.items():
    filename = os.path.join(output_data_dir, view_name + '.csv')
    with open(filename, 'w') as f:
        f.write(','.join(view.columns) + '\n')

# Obtaining the XML tree
with open(os.path.join(input_data_dir, input_filename), 'rb') as f:
    tree = etree.fromstring(f.read(), None)
    #tree = [tree.getchildren()[0]]

for node in tree:
    # Parsing the record
    record.process(node)

    # Postprocessing: author_address bridge table
    postprocess(record)
    
    # Printing entities and views
    for entity_name, entity in record.entity_dict.items():
        if entity_name.startswith('*'):
            continue
        # print(f'Saving entity `{entity_name}`')
        df = entity.to_dataframe()
        df.to_csv(os.path.join(output_data_dir, entity_name + '.csv'), mode='a', index=False, header=False)

    for view_name, view in record.view_dict.items():
        # print(f'Saving view `{view_name}`')
        df = view.to_dataframe()
        df.to_csv(os.path.join(output_data_dir, view_name + '.csv'), mode='a', index=False, header=False)

    record.reset()