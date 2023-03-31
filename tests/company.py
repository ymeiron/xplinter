import os, argparse
from lxml import etree
from xplinter.record_generator import generate_record

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Shred a simple XML document')
    parser.add_argument('--csv', help='Output the tables in CSV format, specify directory name')
    parser.add_argument('--pgsql', action='store_true', help='Output the tables into a PostgreSQL database')
    parser.add_argument('--host', help='Database host name or address for PostgreSQL')
    parser.add_argument('--dbname', help='Database name for PostgreSQL')
    parser.add_argument('--user', help='Database user name for PostgreSQL')
    parser.add_argument('--password', help='Database password name for PostgreSQL')
    args = parser.parse_args()

    if args.csv and not args.pgsql:
        from xplinter.drivers.csv import Csv_driver
        driver = Csv_driver(args.csv, reset=True)
    elif not args.csv and args.pgsql:
        from xplinter.drivers.pgsql import Pgsql_driver
        driver = Pgsql_driver({'host':args.host, 'dbname': args.dbname, 'user': args.user, 'password': args.password}, reset=True)
    else:
        print('Please choose between CSV and PostgreSQL output.')
        raise SystemExit

    my_dir_name = os.path.dirname(os.path.realpath(__file__))
    xplinter_filename = 'company.xplinter'
    with open(os.path.join(my_dir_name, xplinter_filename), 'r') as f: content = f.read()
    record = generate_record(content, driver=driver)

    input_filename = 'tests/company_data.xml'
    with open(input_filename, 'rb') as f:
        tree = etree.fromstring(f.read(), None)

    for node in tree:
        record.process(node)
        record.write()
        record.reset()
