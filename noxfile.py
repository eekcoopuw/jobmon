"""Nox Configuration for jobmon."""
import os
import shutil

import nox
from nox.sessions import Session


src_locations = ["src/jobmon"]
test_locations = ["tests"]

python = "3.8"


@nox.session(python=python, venv_backend="conda")
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or test_locations

    session.conda_install("mysqlclient", "openssl")
    session.install("-e", ".[test,server]")

    # pytest skips. performance tests are a separate nox target
    extra_args = ['-m', "not performance_tests"]

    # pytest mproc
    session.run("pytest", *args, *extra_args)


@nox.session(python=python, venv_backend="conda")
def performance(session: Session) -> None:
    """Run performance tests that take a while."""
    args = session.posargs or test_locations

    session.conda_install("mysqlclient", "openssl")
    session.install("-e", ".[test,server]")

    extra_args = ["-m", "performance_tests"]
    session.run("pytest", *args, *extra_args)


@nox.session(python=python, venv_backend="conda")
def lint(session: Session) -> None:
    """Lint code using various plugins.

    flake8 - a Python library that wraps PyFlakes, pycodestyle and McCabe script.
    flake8-import-order - checks the ordering of your imports.
    flake8-docstrings - extension for flake8 which uses pydocstyle to check docstrings.
    flake8-annotations -is a plugin for Flake8 that detects the absence of PEP 3107-style
    function annotations and PEP 484-style type comments.
    """
    args = session.posargs or src_locations + test_locations
    # TODO: work these in over time?
    # "flake8-black",
    # "darglint",
    # "flake8-bandit"
    session.install("flake8",
                    "flake8-annotations",
                    "flake8-import-order",
                    "flake8-docstrings")
    session.run("flake8", *args)


@nox.session(python=python, venv_backend="conda")
def typecheck(session: Session) -> None:
    """Type check code."""
    # args = session.posargs or src_locations
    session.install("-e", ".")
    session.install("mypy", "types-Flask", "types-requests", "types-PyMySQL", "types-filelock",
                    "types-PyYAML", "types-setuptools", "types-tabulate")
    # session.run("mypy", *args)
    session.run("mypy", "src/jobmon/client/")


@nox.session(python=python, venv_backend="conda")
def docs(session: Session) -> None:
    """Build the documentation."""
    session.conda_install("graphviz")

    session.install("-e", ".[docs,server]")

    autodoc_output = 'docsource/api'
    if os.path.exists(autodoc_output):
        shutil.rmtree(autodoc_output)
    session.run(
        'sphinx-apidoc',
        # output dir
        '-o', autodoc_output,
        # source dir
        'src/jobmon',
        # exclude from autodoc
        'src/jobmon/server/qpid_integration',
        'src/jobmon/server/web/main.py'
    )
    session.run("sphinx-build", "docsource", "out/_html")


@nox.session(python=python, venv_backend="conda")
def distribute(session: Session) -> None:
    session.run("python", "setup.py", "sdist", "bdist_wheel")


@nox.session(python=python, venv_backend="conda")
def clean(session: Session) -> None:
    dirs_to_remove = ['out', 'jobmon_coverage_html_report', 'dist', 'build', '.eggs',
                      '.pytest_cache', 'docsource/api', '.mypy_cache']
    for path in dirs_to_remove:
        if os.path.exists(path):
            shutil.rmtree(path)

    files_to_remove = ['test_report.xml', '.coverage']
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
