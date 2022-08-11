from xplinter.record_generator import Generate_record
from xplinter.drivers import Csv_driver
from lxml import etree

xplinter_filename = 'company.xplinter'
with open(xplinter_filename, 'r') as f:
    content = f.read()
generate_record = Generate_record()
record = generate_record(content, driver=Csv_driver('.'))
record.driver.reset()

input_filename = 'company_data.xml'
with open(input_filename, 'rb') as f:
    tree = etree.fromstring(f.read(), None)

for node in tree:
    record.process(node)
    record.write()
    record.reset()