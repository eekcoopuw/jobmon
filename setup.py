import versioneer
from setuptools import setup

install_requires = [
    'pandas',
    'sqlalchemy',
    'numpy',
    'flask',
    'flask_cors',
    'Flask-SQLAlchemy',
    'requests',
    'psutil',
    'tabulate',
    'tenacity',
    'tblib']

# pip install -e .[dev]
dev_requires = [
    'flake8',
    'nox',
    'cluster_utils',
    'pymysql',
]

# pip install -e .[docs]
docs_requires = [
    'sphinx',
    'sphinx-autodoc-typehints',
    'sphinx_rtd_theme',
]

setup(
    version=versioneer.get_version(),
    name='jobmon',
    description=('A centralized logging and management utility for a batch of'
                 'SGE jobs'),
    url='https://stash.ihme.washington.edu/projects/CC/repos/jobmon',
    author='CentralComp',
    author_email=('tomflem@uw.edu, mlsandar@uw.edu, gphipps@uw.edu, '
                  'cpinho@uw.edu'),
    install_requires=install_requires,
    extras_require={
        'dev': dev_requires,
        'docs': docs_requires,
    },
    packages=['jobmon',
              'jobmon.client',
              'jobmon.client.execution',
              'jobmon.client.execution.scheduler',
              'jobmon.client.execution.strategies',
              'jobmon.client.execution.strategies.sge',
              'jobmon.client.execution.worker_node',
              'jobmon.client.swarm',
              'jobmon.client.templates',
              'jobmon.models',
              'jobmon.requests',
              'jobmon.server',
              'jobmon.server.deployment',
              'jobmon.server.workflow_reaper',
              'jobmon.server.jobmon_client',
              'jobmon.server.jobmon_scheduler',
              'jobmon.server.jobmon_swarm',
              'jobmon.server.jobmon_worker',
              'jobmon.server.visualization_server'
              ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            "jobmon=jobmon.cli:main",
            "jobmon_command=jobmon.client.execution.worker_node.execution_wrapper:main"
        ]})
