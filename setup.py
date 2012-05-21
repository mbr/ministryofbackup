#!/usr/bin/env python
# coding=utf8

import os
import sys

from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(name='ministryofbackup',
      version='0.1',
      description='A tool for making compressed, encrypted, fast and '\
                  'incremental backups to Amazon S3.',
      long_description=read('README.rst'),
      keywords='backup s3 lzma aes',
      author='Marc Brinkmann',
      author_email='git@marcbrinkmann.de',
      url='http://github.com/mbr/ministryofbackup',
      license='MIT',
      packages=find_packages(exclude=['tests']),
      install_requires=['logbook', 'M2Crypto', 'pyliblzma', 'setproctitle',
                        'msgpack-python', 'progressbar', 'remember'],
      scripts=['mobarchive', 'mob'],
     )
