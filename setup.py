import versioneer
from setuptools import setup
import os
import sys
try:
    from autowrap.build import get_WrapperInstall
    HAS_AUTOWRAP = True
except:
    HAS_AUTOWRAP = False


here = os.path.abspath(os.path.dirname(__file__))
package_dir = os.path.join(here, 'jobmon)')

# Make package importable so wrappers can be generated before true installation
sys.path.insert(0, package_dir)

# this could potentially be looked up using an environment variable...
wrapper_build_dir = os.path.join(here, 'build')
wrapper_cfg = os.path.join(here, 'autowrap.cfg')

# Extend the build_py command to create wrappers, if autowrap is installed
vcmds = versioneer.get_cmdclass()

cmds = {}
cmds['sdist'] = vcmds['sdist']
cmds['version'] = vcmds['version']
if HAS_AUTOWRAP:
    cmds['build_py'] = get_WrapperInstall(vcmds['build_py'], wrapper_build_dir,
                                          wrapper_cfg)
else:
    cmds['version'] = vcmds['build_py']


install_requires = [
    'bs4',
    'pandas',
    'sqlalchemy',
    'numpy',
    'pymysql',
    'mysqlclient',
    'pyyaml',
    'pyzmq',
    'drmaa',
    'jsonpickle',
    'cluster-utils'
]

setup(
    version=versioneer.get_version(),
    cmdclass=cmds,
    name='jobmon',
    description=('A centralized logging and management utility for a batch of'
                 'SGE jobs'),
    url='https://stash.ihme.washington.edu/projects/CC/repos/jobmon',
    author='CentralComp',
    author_email='tomflem@uw.edu, mlsandar@uw.edu, gphipps@uw.edu',
    include_package_data=True,
    install_requires=install_requires,
    packages=['jobmon'],
    entry_points={
        'console_scripts': ["jobmon=jobmon.cli:main",
                            "jobmon_command=jobmon.command_context:unwrap",
                            "remote_sleep_and_write=tests.remote_sleep_and_write:main"]})
