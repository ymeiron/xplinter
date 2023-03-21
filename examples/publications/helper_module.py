from typing import Dict, Optional

month_abbrv_dict: Dict[str,int] = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}

def month_parser(month_text: str) -> Optional[int]:
    if month_text:
        return month_abbrv_dict.get(month_text.upper())

def __main__():
    print('This function runs when the module is imported')

