from glob import glob
import os

from setuptools import setup
from setuptools import find_packages

INSTALL_REQUIRES = [
    'configargparse',
    'numpy',
    'pandas',
    'psutil',
    'python_json_logger',
    'pyyaml',
    'requests',
    'scipy',
    'structlog',
    'tabulate',
    'tenacity',
    'tblib',
]

SERVER_REQUIRES = [
    'flask',
    'flask_cors',
    'Flask-SQLAlchemy',
    'elastic-apm[flask]',
    'pymysql',  # install MySQLdb/mysqlclient for more performance
    'python-logstash-async',
    'sqlalchemy',
]

# pip install -e .[test]
TEST_REQUIRES = [
    "pytest",
    "pytest-xdist",
    "pytest-cov",
    "mock",
    "filelock",
    'cluster_utils',
    'tiny_structured_logger',
]

# pip install -e .[docs]
DOCS_REQUIRES = [
    'sphinx==3.3.1',
    'sphinx-autodoc-typehints',
    'sphinx_rtd_theme',
    'graphviz',
    'sphinx_code_tabs',
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
        'server': SERVER_REQUIRES
    },

    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[os.path.splitext(os.path.basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
    package_data={"jobmon": ["py.typed"]},

    setup_requires=["setuptools_scm"],
    use_scm_version={'local_scheme': 'no-local-version',
                     'write_to': 'src/jobmon/_version.py',
                     'fallback_version': '0.0.0',
                     'version_scheme': 'release-branch-semver'},

    entry_points={
        'console_scripts': [
            'jobmon=jobmon.client.cli:main',
            'jobmon_distributor=jobmon.client.distributor.cli:main',
            'jobmon_server=jobmon.server.cli:main [server]',
            'worker_node_entry_point=jobmon.worker_node.cli:run'
        ]
    }
)
