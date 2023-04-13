from typing import Dict, Optional
from lxml.etree import _Element
import datetime

def paragraph_formatter(node: _Element) -> str:
    if len(node):
        text = node[0].text
        for p in node[1:]: text += '\n' + p.text
        return text
    else:
        return ''

def parse_conf_date(date_str: str) -> datetime.date:
    try:
        year  = int(date_str[:4])
        month, day = 1, 1
        if len(date_str) > 4: month = int(date_str[4:6])
        if len(date_str) > 6: day = int(date_str[6:])
        return datetime.date(year, month, day)
    except:
        return None

def parse_conf_admin_div(conf_state: Optional[str]) -> Optional[str]:
    if conf_state and len(conf_state)==2: # it's a US state
        return conf_state

def parse_conf_country(conf_state: Optional[str]) -> Optional[str]:
    if conf_state and len(conf_state)==2: # it's a US state
        return 'USA'
    elif conf_state == 'ELECTR NETWORK':
        return None
    else:
        return conf_state

def parse_conf_online(conf_state: Optional[str]) -> bool:
    return conf_state == 'ELECTR NETWORK'

def parse_ref_year(year_text: Optional[str]) -> int:
    if year_text:
        return int(year_text[:4])

def parse_grant_date(date_str: str) -> Optional[datetime.date]:
    if date_str is None: return None
    if len(date_str) == 10: # with dashes (2023-01-01)
        year  = int(date_str[:4])
        month = int(date_str[5:7])
        day   = int(date_str[8:])
        return datetime.date(year, month, day)
    elif len(date_str) == 8: # without dashes (20230101)
        year  = int(date_str[:4])
        month = int(date_str[4:6])
        day   = int(date_str[6:])
        return datetime.date(year, month, day)
    elif len(date_str) == 6 and date_str.endswith('--'): # Some Japanese grants have this format (2023--)
        year  = int(date_str[:4])
        return datetime.date(year, 1, 1)
    elif len(date_str) == 4: # Some Russian grants have this format (2023)
        year  = int(date_str)
        return datetime.date(year, 1, 1)
    else:
        raise ValueError('Date format not understood')
