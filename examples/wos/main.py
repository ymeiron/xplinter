from xplinter.record_generator import generate_record
from xplinter import Record
from xplinter.drivers.pgsql import Pgsql_driver
from lxml import etree
from typing import IO
import os, gzip, multiprocessing as mp, glob, logging, logging.handlers, psutil, yaml, time, psycopg2

class Preamble_injector:
    def __init__(self, file: IO, preamble: bytes, skip: int = 0):
        self.file = file
        self.file.seek(skip)
        self.preamble = preamble
        self.vpos = 0
    def read(self, size: int = -1) -> bytes:
        if size < 0: raise NotImplementedError
        vpos_begin = self.vpos
        vpos_end   = self.vpos + size # or eof???
        self.vpos  = vpos_end
        if vpos_begin >= len(self.preamble):
            return self.file.read(size)
        elif (vpos_begin < len(self.preamble)) and (vpos_end > len(self.preamble)):
            size2 = size - len(self.preamble) + vpos_begin
            result = self.preamble[vpos_begin:]
            result += self.file.read(size2)
            return result
        elif vpos_end <= len(self.preamble):
            return self.preamble[vpos_begin:vpos_end]
        print(vpos_begin, vpos_end, size)
        raise RuntimeError # Shouldn't be here

def postprocess(record: Record) -> None:
    author_address = record.entity_dict['author_address']
    author         = record.entity_dict['author']
    address        = record.entity_dict['address']
    for row in author.data:
        try:
            author_id = row[0]
            addr_no_list = row[11].split()
            for addr_no in addr_no_list:
                address_id = address.data[int(addr_no) - 1][0]
                author_address.data.append([author_id, address_id])
        except:
            pass

def finalize():
    con = psycopg2.connect(**config['db_config'])
    con.set_session(autocommit=True)
    cur = con.cursor()

    with open(os.path.join(my_dir_name, 'finalize.sql'), 'r') as f:
        content = f.read()

    blocks = ['']
    lines = content.splitlines()
    for line in lines:
        if line.startswith('-- @xplinter'):
            blocks.append(line)
            blocks.append('')
        elif line.startswith('--'):
            continue
        else:
            blocks[-1] += line + '\n'

    for block in blocks:
        block = block.strip()
        if block.startswith('-- @xplinter'):
            logger.log(logging.INFO, block[13:])
        elif len(block) > 0:
            cur.execute(block)

def worker(rank: int):
    logger.log(logging.INFO, f'[worker {rank:02d}] Started')
    record = generate_record(wos_xplinter, driver=Pgsql_driver(config['db_config'], max_records=config['internal_max_records'], reset=False), logger=logger)
    if rank == 0:
        logger.log(logging.INFO, f'Resetting the database')
        record.driver.reset()
    barrier.wait()

    while True:
        filename = queue.get()
        if filename is None:
            logger.log(logging.INFO, f'No more work in queue')
            break
        logger.log(logging.INFO, f'Starting to process file: {os.path.basename(filename)}')
        with gzip.open(filename, 'rb') as gzipfile:
            f = Preamble_injector(gzipfile, b'<?xml version="1.0" encoding="UTF-8"?>\n<records>\n', skip=176)
            for _, node in etree.iterparse(f):
                if node.tag == 'REC':
                    record.process(node)
                    postprocess(record)
                    start = time.perf_counter_ns()
                    record.write()
                    end = time.perf_counter_ns()
                    write_times[rank] += end - start
                    record_counts[rank] += 1
                    node.clear()
                    node.getparent().remove(node)
        logger.log(logging.INFO, f'Finished processing XML file')
    record.driver.close()
    logger.log(logging.INFO, f'Finished')

if __name__ == '__main__':
    my_dir_name = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(my_dir_name, 'config.yaml'), 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    with open(os.path.join(my_dir_name, 'wos.xplinter'), 'r') as f:
        wos_xplinter = f.read()

    barrier       = mp.Barrier(config['n_workers'])
    queue         = mp.Manager().Queue()
    record_counts = mp.Array('Q', [0]*config['n_workers']) # number of records written by each process
    write_times   = mp.Array('Q', [0]*config['n_workers']) # time in nanoseconds spent on record.write()

    logger = logging.getLogger()
    file_handler = logging.FileHandler(config['log_filename'], 'w')
    formatter = logging.Formatter('%(asctime)s [%(process)d] %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    logger.log(logging.INFO, '[main] WoS shredding application started')

    filenames = sorted(glob.glob(os.path.join(config['input_data_dir'], config['glob'])))
    if config.get('reverse'): filenames = filenames[::-1]
    for filename in filenames: queue.put(filename)
    for rank in range(config['n_workers']): queue.put(None)

    processes = [mp.Process(target=worker, args=(rank,)) for rank in range(config['n_workers'])]
    for p in processes: p.start()
    process_infos = [psutil.Process(p.pid) for p in processes]

    time.sleep(config['log_interval'])
    while True:
        start = time.perf_counter_ns()
        workers_alive = sum([p.is_alive() for p in processes])
        if not workers_alive:
            break

        cpu_percent_list, io_counters_list, read_ops, read_bytes = [], [], [], []
        for i, p in enumerate(process_infos):
            try:
                cpu_percent = p.cpu_percent()
                cpu_percent_list.append(cpu_percent)
            except:
                cpu_percent_list.append(0.)
            try:
                io_counters = p.io_counters()
                read_ops.append(io_counters.read_count)
                read_bytes.append(io_counters.read_bytes)
            except:
                read_ops.append(0)
                read_bytes.append(0)

        cpu_percent = psutil.cpu_percent(percpu=False)
        mem = psutil.virtual_memory().used/1024**3
        logging.info(f'cpus: {cpu_percent_list}')
        logging.info(f'read ops: {read_ops}')
        logging.info(f'read bytes: {read_bytes}')
        logging.info(f'write times: {write_times[:]}')
        counter = sum(record_counts)
        logging.info(f'counter: {counter: 9d}, live workers: {workers_alive: 2d}, sys cpu usage: {cpu_percent:5.2f}%, sys mem usage: {mem:5.2f} GiB')
        end = time.perf_counter_ns()
        elapsed = (end - start)/1E9
        time.sleep(max(config['log_interval'] - elapsed, 0))

    #for p in processes: p.join() # Actually redundant because we wait all workers to terminate

    counter = sum(record_counts)
    logging.info(f'counter: {counter: 9d}')
    logging.info(f'Now executing finalizing SQL commands')
    finalize()
    logging.info(f'All done')
