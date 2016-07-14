#!/usr/bin/env python
from pip.req import parse_requirements

install_reqs = parse_requirements("requirements.txt", session=False)

reqs = [str(ir.req) for ir in install_reqs]
sdk_package_name = 'alooma_pysdk'

from distutils.core import setup

setup(
    name=sdk_package_name,
    packages=[sdk_package_name],
    package_data={sdk_package_name: ['alooma_ca']},
    version='2.0',
    description='An easy-to-integrate SDK for your Python apps to report '
                'events to Alooma',
    url='https://github.com/Aloomaio/python-sdk',
    author='Yuval Barth',
    author_email='yuval@alooma.com',
    keywords=['python', 'sdk', 'alooma', 'pysdk'],
    install_requires=install_reqs
)
