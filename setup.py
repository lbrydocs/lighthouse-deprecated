from lighthouse import __version__
from setuptools import setup, find_packages
import os

base_dir = os.path.abspath(os.path.dirname(__file__))

console_scripts = ['start-lighthouse = lighthouse.Control:start',
                   'silenceinthelbry = lighthouse.Control:stop',
                   'lighthouse-cli = lighthouse.Control:cli']

requires = ['lbrynet', 'fuzzywuzzy', 'python-Levenshtein']

setup(name='lighthouse',
      description='Basic search engine for publications on the lbrycrd blockchain',
      version=__version__,
      maintainer='Jack Robison',
      maintainer_email='jackrobison@lbry.io',
      install_requires=requires,
      packages=find_packages(base_dir),
      entry_points={'console_scripts': console_scripts},
      dependency_links=['https://github.com/lbryio/lbryum/tarball/master/#egg=lbryum'],
      )
