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
    'pandas',
    'sqlalchemy',
    'numpy',
    'flask',
    'flask_cors',
    'flask_sqlalchemy',
    'cluster_utils',
    'requests',
    'paramiko',
    'graphviz',
    'tenacity'
]


setup(
    version=versioneer.get_version(),
    cmdclass=cmds,
    name='jobmon',
    description=('A centralized logging and management utility for a batch of'
                 'SGE jobs'),
    url='https://stash.ihme.washington.edu/projects/CC/repos/jobmon',
    author='CentralComp',
    author_email=('tomflem@uw.edu, mlsandar@uw.edu, gphipps@uw.edu, '
                  'cpinho@uw.edu'),
    include_package_data=True,
    install_requires=install_requires,
    packages=['jobmon',
              'jobmon.client',
              'jobmon.client.swarm',
              'jobmon.client.swarm.executors',
              'jobmon.client.swarm.job_management',
              'jobmon.client.swarm.workflow',
              'jobmon.client.worker_node',
              'jobmon.models',
              'jobmon.models.attributes',
              'jobmon.server',
              'jobmon.server.health_monitor',
              'jobmon.server.job_query_server',
              'jobmon.server.job_state_manager',
              'jobmon.server.job_visualization_server'
              ],
    entry_points={
        'console_scripts': [
            "jobmon=jobmon.cli:main",
            "jobmon_command=jobmon.client.worker_node.cli:unwrap"]})
