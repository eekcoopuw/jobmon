import os

from setuptools import setup


def read(read_file: str):
    return open(os.path.join(os.path.dirname(__file__), read_file)).read()


setup(
    name='ihme_client'

)
