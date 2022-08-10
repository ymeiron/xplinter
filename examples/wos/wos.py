from xplinter.record_generator import Generate_record
from xplinter import Record
from lxml import etree
import os

output_data_dir = 'output_data'
input_data_dir  = 'input_data'
input_filename  = 'wos_long_sample.xml'

from xplinter.drivers import Csv_driver

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
record = generate_record(content, driver=Csv_driver(output_data_dir))

# Obtaining the XML tree
with open(os.path.join(input_data_dir, input_filename), 'rb') as f:
    tree = etree.fromstring(f.read(), None)
    #tree = [tree.getchildren()[0]]

for node in tree:
    record.process(node)
    postprocess(record)
    record.write()
    record.reset()