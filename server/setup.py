from glob import glob
import os

from setuptools import setup, find_namespace_packages


thisdir = os.path.dirname(os.path.realpath(os.path.expanduser(__file__)))


GIT_DESCRIBE_COMMAND = [
    "git",
    "--git-dir",
    os.path.join(thisdir, "..", ".git"),
    "describe",
    "--dirty",
    "--tags",
    "--long",
    "--match",
    "server-[0-9]*",  # only match on jobmon_slurm_plugin tags
]

INSTALL_REQUIRES = [
    'jobmon',
    'flask',
    'flask_cors',
    'elastic-apm[flask]',
    'pymysql',  # install MySQLdb/mysqlclient for more performance
    'sqlalchemy',
    'structlog',
    'scipy',
]

# pip install -e .[test]
TEST_REQUIRES = [
    "pytest",
    "pytest-cov",
    "pytest-xdist",
    "jobmon[server]",
    "pymysql",
    "filelock",
]


def read(read_file: str):
    return open(os.path.join(os.path.dirname(__file__), read_file)).read()


setup(
    name='jobmon_server',
    maintainer='Geoffrey Phipps',
    maintainer_email='gphipps@uw.edu',
    url='https://stash.ihme.washington.edu/projects/SCIC/repos/jobmon_slurm',
    description='Web Service and database for jobmon.',
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
    },

    packages=find_namespace_packages(where='src', include=["jobmon.*"]),
    package_dir={'': "src"},
    py_modules=[
        os.path.splitext(os.path.basename(path))[0]
        for path in glob('server/src/*.py')
    ],
    package_data={"jobmon/server": ["py.typed"]},

    setup_requires=["setuptools_scm"],
    use_scm_version={
        'root': '..',
        'local_scheme': 'no-local-version',
        'write_to': os.path.join(thisdir, 'src/jobmon/server/_version.py'),
        'version_scheme': 'release-branch-semver',
        'git_describe_command': GIT_DESCRIBE_COMMAND
    },

    entry_points={
        'console_scripts': [
            'jobmon_server=jobmon.server.cli:main [server]',
        ]
    }
)
