from lighthouse import __version__
from setuptools import setup, find_packages
import os

base_dir = os.path.abspath(os.path.dirname(__file__))

console_scripts = ['lighthouse-start = lighthouse.Control:start',
                   'lighthouse-stop = lighthouse.Control:stop',
                   'lighthouse-cli = lighthouse.Control:cli']

requires = [
      'lbrynet',
      'fuzzywuzzy',
      'python-Levenshtein',
      'PyYAML',
      'python-bitcoinrpc',
      'lbryschema==0.0.4',
      'lbryum==2.7.21',
      'lbrynet==0.10.2',
      'lbryum-server==1.0.2'
]

setup(name='lighthouse',
      description='Basic search engine for publications on the lbrycrd blockchain',
      version=__version__,
      maintainer='Jack Robison',
      maintainer_email='jackrobison@lbry.io',
      install_requires=requires,
      packages=find_packages(base_dir, exclude=['tests']),
      entry_points={'console_scripts': console_scripts},
      )
