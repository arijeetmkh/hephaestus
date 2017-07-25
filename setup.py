#!/usr/bin/env python

from setuptools import setup
import hephaestus

setup(
    name='hephaestus',
    version=hephaestus.__version__,
    description='Queue Consumer',
    author='Arijeet Mukherjee',
    author_email='arijeet.mkh@gmail.com',
    url="https://github.com/arijeetmkh/hephaestus",
    packages=['hephaestus'],
    install_requires=['boto3', 'configparser', 'cerberus'],
    scripts=['bin/hephaestus'],
    package_data={
        'hephaestus': ['hephaestus.conf', 'message_transport_conf.json']
    },
    zip_safe=False
)