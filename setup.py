#!/usr/bin/env python
from pip.req import parse_requirements

install_reqs = parse_requirements("requirements.txt", session=False)

reqs = [str(ir.req) for ir in install_reqs]
sdk_package_name = 'alooma_pysdk'

from distutils.core import setup

setup(
  name = sdk_package_name,
  packages = [sdk_package_name],
  package_data = {sdk_package_name: ['alooma_ca']},
  version = '0.1.1.2',
  description = 'A Python SDK to embed in your Python app to send events to your Alooma server',
  url = 'https://github.com/Aloomaio/python-sdk',
  author = 'Yuval Barth',
  author_email = 'yuval@alooma.com',
  keywords = ['python', 'sdk', 'alooma'],
  install_requires = install_reqs
)

