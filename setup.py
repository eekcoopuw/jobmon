import versioneer
import os
import sys
from setuptools import setup


here = os.path.abspath(os.path.dirname(__file__))
package_dir = os.path.join(here, 'jobmon)')

# Make package importable so wrappers can be generated before true installation
sys.path.insert(0, package_dir)

# Extend the build_py command to create wrappers, if autowrap is installed
vcmds = versioneer.get_cmdclass()

cmds = {}
cmds['sdist'] = vcmds['sdist']
cmds['version'] = vcmds['version']
cmds['version'] = vcmds['build_py']


install_requires = [
    'pandas',
    'sqlalchemy',
    'numpy',
    'flask',
    'flask_cors',
    'Flask-SQLAlchemy',
    'cluster_utils',
    'requests',
<<<<<<< HEAD
    'tabulate',
    'tenacity',
    'tblib']
=======
    'paramiko',
    'tabulate',
    'tenacity']
>>>>>>> d8544f7b2444c25a98a75878093647681596e6bb

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
    install_requires=install_requires,
    packages=['jobmon',
              'jobmon.client',
              'jobmon.client.execution',
              'jobmon.client.execution.scheduler',
              'jobmon.client.execution.strategies',
              'jobmon.client.execution.strategies.sge',
              'jobmon.client.execution.worker_node',
              'jobmon.client.requests',
              'jobmon.client.swarm',
              'jobmon.client.templates',
              'jobmon.models',
              'jobmon.models.attributes',
              'jobmon.server',
              'jobmon.server.deployment',
              'jobmon.server.health_monitor',
              'jobmon.server.query_server',
              'jobmon.server.update_server',
              'jobmon.server.job_visualization_server'
              ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            "jobmon=jobmon.cli:main",
            "jobmon_command=jobmon.client.worker_node.execution_wrapper:main"
        ]})
