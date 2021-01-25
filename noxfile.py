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
    session.install("-e", ".[test]")

    # pytest skips. performance tests are a separate nox target
    skip_string = "not performance_tests and not integration_tests "
    try:
        os.environ['SGE_ENV']
    except KeyError:
        skip_string += "and not integration_sge"
    extra_args = ['-m', skip_string]

    # pytest mproc
    session.run("pytest", *args, *extra_args)


@nox.session(python=python, venv_backend="conda")
def integration(session: Session) -> None:
    """Run integration tests that take a while."""
    args = session.posargs or test_locations

    session.conda_install("mysqlclient", "openssl")
    session.install("-e", ".[test]")

    extra_args = ["-m", "integration_tests"]
    session.run("pytest", *args, *extra_args)


@nox.session(python=python, venv_backend="conda")
def performance(session: Session) -> None:
    """Run performance tests that take a while."""
    args = session.posargs or test_locations

    session.conda_install("mysqlclient", "openssl")
    session.install("-e", ".[test]")

    extra_args = ["-m", "performance_tests"]
    session.run("pytest", *args, *extra_args)


@nox.session(python=python, venv_backend="conda")
def lint(session: Session) -> None:
    """Lint code using various plugins."""
    args = session.posargs or src_locations + test_locations
    # TODO: work these in over time?
    # "flake8-black",
    # "flake8-import-order",
    # "flake8-annotations",
    # "flake8-docstrings",
    # "darglint",
    session.install("flake8")
    session.run("flake8", *args)


@nox.session(python=python, venv_backend="conda")
def typecheck(session: Session) -> None:
    """Type check code."""
    args = session.posargs or src_locations + test_locations
    session.install("mypy")
    session.run("mypy", *args)


@nox.session(python=python, venv_backend="conda")
def docs(session: Session) -> None:
    """Build the documentation."""
    session.install("-e", ".[docs]")
    autodoc_output = 'docsource/api'
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
