from glob import glob
import os

from setuptools import setup
from setuptools import find_packages

INSTALL_REQUIRES = [
    'configargparse',
    'numpy',
    'pandas',
    'psutil',
    'pyyaml',
    'requests',
    'tabulate',
    'tenacity',
    'typing_extensions'  # TODO: remove when we no longer support 3.7
]

SERVER_REQUIRES = [
    'flask',
    'flask_cors',
    'elastic-apm[flask]',
    'pymysql',  # install MySQLdb/mysqlclient for more performance
    'python-logstash-async',
    'slurm_rest',  # TODO: when the integrator is split out, remove this dependency
    'sqlalchemy',
    'python_json_logger',
    'structlog',
    'scipy==1.8.1',  # Pinned because scipy 1.9.0 is not compatible with uwsgi for some reason.
]

# pip install -e .[test]
TEST_REQUIRES = [
    "pytest",  
    "pytest-xdist",
    "pytest-cov",
    "mock",
    "filelock",
    'cluster_utils',
]

# pip install -e .[docs]
DOCS_REQUIRES = [
    'sphinx',
    'sphinx-autodoc-typehints',
    'sphinx_rtd_theme',
    'graphviz',
    'sphinx_tabs',
]

IHME_REQUIRES = [
    'jobmon_installer_ihme'
]


def read(read_file: str):
    return open(os.path.join(os.path.dirname(__file__), read_file)).read()


# TODO: consider splitting into 3 builds: jobmon_server, jobmon_client, jobmon.
# Subclass install to accept parameters https://stackoverflow.com/questions/18725137/how-to-obtain-arguments-passed-to-setup-py-from-pip-with-install-option
setup(
    name='jobmon',
    maintainer='IHME SciComp',
    maintainer_email='gphipps@uw.edu',
    url='https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon',
    description='A dependency management utility for batch computation.',
    long_description=read("README.md"),
    long_description_content_type="text/markdown",

    classifiers="""
        Programming Language :: Python :: 3.9
        Programming Language :: Python :: 3.8
        Programming Language :: Python :: 3.7
        """,

    python_requires='>=3.7',
    install_requires=INSTALL_REQUIRES,
    extras_require={
        'test': TEST_REQUIRES,
        'docs': DOCS_REQUIRES,
        'server': SERVER_REQUIRES,
        'ihme': IHME_REQUIRES,
    },

    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[os.path.splitext(os.path.basename(path))[0] for path in glob('src/*.py')],
    package_data={"jobmon": ["py.typed", "defaults.ini"]},

    use_scm_version={'local_scheme': 'no-local-version',
                     'write_to': 'src/jobmon/_version.py',
                     'fallback_version': '0.0.0',
                     'version_scheme': 'release-branch-semver'},

    entry_points={
        'console_scripts': [
            'jobmon=jobmon.client.cli:main',
            'jobmon_config=jobmon.configuration:main',
            'jobmon_distributor=jobmon.client.distributor.cli:main',
            'jobmon_server=jobmon.server.cli:main [server]',
            'worker_node_entry_point=jobmon.worker_node.cli:run'
        ]
    }
)
