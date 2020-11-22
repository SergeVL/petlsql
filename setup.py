#! /usr/bin/env python
from setuptools import setup, find_packages

setup(name='petlsql',
      description='sql on petl tables',
      author='Serge Voloshenyuk',
      author_email='voloshenyuk@gmail.com',
      version='0.9',
      install_requires=["petl"],
      packages=find_packages()
)

