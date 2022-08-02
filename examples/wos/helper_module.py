from typing import Dict, Optional
from lxml.etree import _Element
from datetime import datetime

class Month_parser:
    month_abbrv_dict: Dict[str,int] = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
    def __call__(self, month: str) -> Optional[int]:
        if month:
            if month[:3] in Month_parser.month_abbrv_dict:
                return Month_parser.month_abbrv_dict[month[:3]]

def paragraph_formatter(node: _Element) -> str:
    if len(node):
        text = node[0].text
        for p in node[1:]: text += '\n' + p.text
        return text
    else:
        return ''

def parse_date(date_str: str) -> int:
    year  = int(date_str[:4])
    month, day = 1, 1
    if len(date_str) > 4: month = int(date_str[4:6])
    if len(date_str) > 6: day = int(date_str[6:])
    d = datetime(year, month, day) - datetime(2000,1,1)
    return int(d.days)

def parse_conf_admin_div(conf_state: Optional[str]) -> str:
    if conf_state and len(conf_state)==2: # it's a US state
        return conf_state

def parse_conf_country(conf_state: Optional[str]) -> str:
    if conf_state and len(conf_state)==2: # it's a US state
        return 'USA'
    elif conf_state == 'ELECTR NETWORK':
        return None
    else:
        return conf_state

def parse_conf_online(conf_state: Optional[str]) -> bool:
    return conf_state == 'ELECTR NETWORK'