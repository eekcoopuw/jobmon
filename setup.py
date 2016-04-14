import os
from setuptools import setup
from codecs import open

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
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
        'pyzmq',
        'setuptools_scm'],
    package_data={'jobmon': ['*.sh']},
    packages=['jobmon'],
    entry_points={
            'console_scripts': [
                'env_submit_master = jobmon.env_submit_master',
                'submit_master = jobmon.submit_master'
            ]
        }
    )
