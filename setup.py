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
    'tblib',
    'configargparse']

# pip install -e .[dev]
dev_requires = [
    'flake8',
    'nox',
    'cluster_utils',
    'pymysql'
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
    description='A logging and dependency management utility for batch computation',
    url='https://stash.ihme.washington.edu/projects/CC/repos/jobmon',
    author='IHME SciComp',
    author_email=('gphipps@uw.edu, mlsandar@uw.edu, cpinho@uw.edu, tomflem@uw.edu'),
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
              'jobmon.server',
              'jobmon.server.deployment',
              'jobmon.server.workflow_reaper',
              'jobmon.server.web.jobmon_client',
              'jobmon.server.web.jobmon_scheduler',
              'jobmon.server.web.jobmon_swarm',
              'jobmon.server.web.jobmon_worker',
              'jobmon.server.web.visualization_server'],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'jobmon=jobmon.client.cli:main',
            'jobmon_scheduler=jobmon.client.execution.cli:main',
            'jobmon_server=jobmon.server.cli:main',
            'jobmon_command=jobmon.client.execution.worker_node.execution_wrapper:main'
        ]
    }
)
