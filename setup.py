#!/usr/bin/env python
from pip.req import parse_requirements
from distutils.core import setup

install_reqs = parse_requirements("requirements.txt", session=False)

reqs = [str(ir.req) for ir in install_reqs]

setup(
  name = 'alooma_pysdk',
  packages = ['alooma_pysdk'],
  version = '0.1',
  description = 'A Python SDK to embed in your Python app to send events to your Alooma server',
  url = 'https://github.com/Aloomaio/python-sdk',
  author = 'Yuval Barth',
  author_email = 'yuval@alooma.com',
  keywords = ['python', 'sdk', 'alooma'],
  install_requires = ['six==1.10.0']
)

