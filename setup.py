#!/usr/bin/env python
from distutils.core import setup
import re

with open('xplinter/_version.py', 'r') as f:
    __version__ = re.match(r"""__version__ = ["'](.*?)['"]""", f.read()).group(1)

setup(name='xplinter',
    version=__version__,
    description='An easy-to-use XML shredder',
    author='Yohai Meiron',
    author_email='yohai.meiron@scinet.utoronto.ca',
    packages=['xplinter', 'xplinter.drivers'],
    package_data={
        '': ['xplinter.lark']
    }
)