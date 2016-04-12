from setuptools import setup

setup(
    name='jobmon',
    version='0.1.0',
    description="Database-backed job monitor",
    url='https://stash.ihme.washington.edu/users/tomflem/repos/jobmon',
    author='',
    author_email='',
    install_requires=[
        'pandas',
        'sqlalchemy',
        'numpy',
        'pymysql',
        'pyzmq'],
    package_data={'jobmon': ['*.sh']},
    packages=['jobmon'])
