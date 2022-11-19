from xplinter.record_generator import Generate_record
from xplinter.drivers import Csv_driver, Pgsql_driver
from lxml import etree
import os

xplinter_filename = 'publications.xplinter'
with open(xplinter_filename, 'r') as f:
    content = f.read()
generate_record = Generate_record()
record = generate_record(content, driver=Csv_driver('output'))
record.driver.reset()

input_directory = 'sources'
file_list = os.listdir(input_directory)
file_list = sorted([filename for filename in file_list if filename.endswith('.xml')])
for i, input_filename in enumerate(file_list):
    input_filename = os.path.join(input_directory, input_filename)
    print(i, input_filename)
    with open(input_filename, 'rb') as f:
        tree = etree.fromstring(f.read(), None)
    record.process(tree)
    record.write()
    record.reset()