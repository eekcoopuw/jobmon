from setuptools import setup
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='jobmon',
    use_scm_version=True,
    description="Database-backed job monitor",
    long_description=long_description,
    url='',
    author='',
    author_email='',
    install_requires=[
        'pandas',
        'sqlalchemy',
        'numpy',
        'pymysql',
        'pyzmq'
        'setuptools_scm'],
    package_data={'jobmon': ['*.sh']},
    packages=['jobmon'])
