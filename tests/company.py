import sys; sys.path.append('../xplinter')
from xplinter.record_generator import Generate_record
from xplinter.drivers import Csv_driver, Pgsql_driver
from lxml import etree

xplinter_filename = 'tests/company.xplinter'
with open(xplinter_filename, 'r') as f:
    content = f.read()
generate_record = Generate_record()
record = generate_record(content, driver=Pgsql_driver({'host':'idb1', 'dbname':'ymeiron_db0'}))
#record = generate_record(content, driver=Csv_driver('tmpdirdel'))
record.driver.reset()

input_filename = 'tests/company_data.xml'
with open(input_filename, 'rb') as f:
    tree = etree.fromstring(f.read(), None)

for node in tree:
    record.process(node)
    record.write()
    record.reset()

#record.driver.close()