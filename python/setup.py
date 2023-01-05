from glob import glob
import os

from setuptools import setup
from setuptools import find_packages

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
    "python-[0-9]*",  # only match on jobmon_slurm_plugin tags
]

INSTALL_REQUIRES = [
    'configparser',
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
    'jobmon_server'
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
    py_modules=[
        os.path.splitext(os.path.basename(path))[0]
        for path in glob('python/src/*.py')
    ],
    package_data={"jobmon": ["py.typed", "defaults.ini"]},

    use_scm_version={
        'root': '..',
        'local_scheme': 'no-local-version',
        'write_to': os.path.join(thisdir, 'src/jobmon/_version.py'),
        'version_scheme': 'release-branch-semver',
        'git_describe_command': GIT_DESCRIBE_COMMAND
    },

    entry_points={
        'console_scripts': [
            'jobmon=jobmon.client.cli:main',
            'jobmon_config=jobmon.configuration:main',
            'jobmon_distributor=jobmon.client.distributor.cli:main',
            'worker_node_entry_point=jobmon.worker_node.cli:run'
        ]
    }
)
