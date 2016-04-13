import subprocess
import os
from setuptools import setup
from setuptools.command.install import install
from codecs import open

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


class FixPerms(install):
    def run(self):
        # run normal install
        install.run(self)

        # fix permissions on shell files
        dir_path = os.path.dirname(os.path.realpath(__file__))
        subprocess.call(['chmod', '555', dir_path + "/env_submit_master.sh"])
        subprocess.call(['chmod', '555', dir_path + "/submit_master.sh"])


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
    cmdclass={'install': FixPerms},
    packages=['jobmon'])
