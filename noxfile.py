"""Nox Configuration for jobmon."""
import os
import nox
from nox.sessions import Session


src_locations = ["jobmon"]
test_locations = ["tests"]


@nox.session(python="3.7", venv_backend="conda")
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or test_locations
    session.conda_install("mysqlclient")
    session.conda_install("-y", "-c", "conda-forge", "openssl=1.0.2p")
    session.install("pytest", "pytest-xdist")
    session.install("-r", "requirements.txt")
    session.install("--upgrade", "--force-reinstall", ".")

    try:
        os.environ['SGE_ENV']
    except KeyError:
        args.extend(["-m", "not integration_sge"])
    session.run("pytest", *args)


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
