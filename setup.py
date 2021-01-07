from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import setup
from setuptools import find_packages

INSTALL_REQUIRES = [
    'configargparse',
    'flask',
    'flask_cors',
    'Flask-SQLAlchemy',
    'numpy',
    'pandas',
    'psutil',
    'pymysql',  # install MySQLdb/mysqlclient for more performance
    'python_json_logger',
    'requests',
    'sqlalchemy',
    'structlog',
    'tabulate',
    'tenacity',
    'tblib',
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
]


def local_scheme(version):
    """Skip the local version (eg. +xyz of 0.6.1.dev4+gdf99fe2)
    to be able to upload to Test PyPI"""
    return ""


# TODO: consider splitting into 3 builds: jobmon_server, jobmon_client, jobmon.
# Subclass install to accept parameters https://stackoverflow.com/questions/18725137/how-to-obtain-arguments-passed-to-setup-py-from-pip-with-install-option
setup(
    name='jobmon',
    maintainer='IHME SciComp',
    maintainer_email='gphipps@uw.edu',
    url='https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon',
    description='A dependency management utility for batch computation.',
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",

    classifiers="""
        Programming Language :: Python :: 3.7
        Programming Language :: Python :: 3.8
        """,

    python_requires='>=3.7',
    install_requires=INSTALL_REQUIRES,
    extras_require={
        'test': TEST_REQUIRES,
        'docs': DOCS_REQUIRES,
    },

    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,

    setup_requires=["setuptools_scm"],
    use_scm_version={'local_scheme': local_scheme,
                     'write_to': 'src/jobmon/_version.py',
                     'fallback_version': '0.0.0'},

    entry_points={
        'console_scripts': [
            'jobmon=jobmon.client.jobmon_cli:main',
            'jobmon_scheduler=jobmon.client.execution.cli:main',
            'jobmon_server=jobmon.server.web.jobmon_cli:main',
            'jobmon_command=jobmon.client.execution.worker_node.execution_wrapper:main'
        ]
    }
)
