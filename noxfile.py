"""Nox Configuration for jobmon."""
import os
import sys

import nox
from nox.sessions import Session


src_locations = ["jobmon"]
test_locations = ["tests"]

python = "3.7"


@nox.session(python=python, venv_backend="conda")
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or test_locations
    session.conda_install("mysqlclient")
    if python == "3.7":
        session.conda_install("-y", "-c", "conda-forge", "openssl=1.0.2p")
    session.install("pytest", "pytest-mproc<=3.2.9", "mock")
    session.install("-r", "requirements.txt")
    session.install("-e", ".")

    # pytest sge integration tests
    try:
        os.environ['SGE_ENV']
        extra_args: list = []
    except KeyError:
        extra_args = ["-m", "not integration_sge"]

    # pytest mproc
    disable_mproc = ["--disable-mproc", "True"]
    if "--cores" not in args:
        extra_args.extend(disable_mproc)
    session.run("pytest", *args, *extra_args)


@nox.session(python="3.7", venv_backend="conda")
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


@nox.session(python="3.7", venv_backend="conda")
def typecheck(session: Session) -> None:
    """Type check code."""
    args = session.posargs or src_locations + test_locations
    session.install("mypy")
    session.run("mypy", *args)


@nox.session(python="3.7", venv_backend="conda")
def docs(session: Session) -> None:
    """Build the documentation."""
    session.install(".")
    session.install("sphinx", "sphinx-autodoc-typehints",
                    "sphinx_rtd_theme")
    session.run("sphinx-build", "docsource", "docsource/_build")


@nox.session(python="3.7", venv_backend="conda")
def build(session: Session) -> None:
    session.run("python", "setup.py", "sdist")


@nox.session(python="3.8", venv_backend="conda")
def release(session: Session) -> None:
    """Release the distribution."""
    # check if overwrite is an arg
    args = session.posargs
    if "--overwrite" in args:
        cmd: tuple = ("release", "--project_name", "jobmon", "--overwrite")
    else:
        cmd = ("release", "--project_name", "jobmon")

    session.install(
        "deploytools",
        "--extra-index-url",
        "https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple",
        "--trusted-host",
        "artifactory.ihme.washington.edu")
