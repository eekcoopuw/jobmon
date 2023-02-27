"""Nox Configuration for Jobmon."""
import glob
import os
from pathlib import Path
import shutil


import nox
from nox.sessions import Session


src_locations = ["jobmon_client/src", "jobmon_core/src", "jobmon_server/src"]
test_locations = ["tests"]

python = "3.8"


@nox.session(python=python, venv_backend="conda")
def tests(session: Session) -> None:
    """Run the test suite."""
    session.conda_install("mysqlclient")
    session.install("pytest", "pytest-xdist", "pytest-cov", "mock", "filelock")
    session.install("-e", "./jobmon_core")
    session.install("-e", "./jobmon_client")
    session.install("-e", "./jobmon_server")

    args = session.posargs or test_locations

    session.run(
        "pytest",
        "--cov=jobmon",
        "--cov-report=html",
        *args,
        env={"SQLALCHEMY_WARN_20": "1"}
    )


@nox.session(python=python, venv_backend="conda")
def lint(session: Session) -> None:
    """Lint code using various plugins.

    flake8 - a Python library that wraps PyFlakes, pycodestyle and McCabe script.
    flake8-import-order - checks the ordering of your imports.
    flake8-docstrings - extension for flake8 which uses pydocstyle to check docstrings.
    flake8-annotations -is a plugin for Flake8 that detects the absence of PEP 3107-style
    function annotations and PEP 484-style type comments.
    """
    args = session.posargs or src_locations
    # TODO: work these in over time?
    # "darglint",
    # "flake8-bandit"
    session.install(
        "flake8",
        "flake8-annotations",
        "flake8-import-order",
        "flake8-docstrings",
        "flake8-black"
    )
    session.run("flake8", *args)


@nox.session(python=python, venv_backend="conda")
def black(session):
    args = session.posargs or src_locations + test_locations
    session.install("black")
    session.run("black", *args)


@nox.session(python=python, venv_backend="conda")
def typecheck(session: Session) -> None:
    """Type check code."""
    args = session.posargs or src_locations
    session.install("mypy", "types-Flask", "types-requests", "types-PyMySQL", "types-filelock",
                    "types-PyYAML", "types-tabulate", "types-psutil", "types-Flask-Cors",
                    "types-sqlalchemy-utils", "types-pkg-resources", "types-mysqlclient")

    session.install("-e", "./jobmon_core")
    session.install("-e", "./jobmon_client")
    session.install("-e", "./jobmon_server")

    session.run("mypy", "--explicit-package-bases", *args)


@nox.session(python=python, venv_backend="conda")
def docs(session: Session) -> None:
    """Build the documentation."""
    # environment variables used in build script
    web_service_fqdn = \
        os.environ.get("WEB_SERVICE_FQDN") if "WEB_SERVICE_FQDN" in os.environ else "TBD"
    web_service_port = \
        os.environ.get("WEB_SERVICE_PORT") if "WEB_SERVICE_PORT" in os.environ else "TBD"

    session.conda_install("graphviz")
    session.install(
        "sphinx",
        "sphinx-autodoc-typehints",
        "sphinx_rtd_theme",
        "sphinx_tabs",
    )

    # combine source into one directory by installing
    session.install("./jobmon_core")
    session.install("./jobmon_client")
    session.install("./jobmon_server")
    install_path = (
        Path(session.virtualenv.location)
        / "lib"
        / f"python{session.python}"
        / "site-packages"
        / "jobmon"
    )

    # generate api docs
    autodoc_output = 'docsource/api'
    if os.path.exists(autodoc_output):
        shutil.rmtree(autodoc_output)
    session.run(
        'sphinx-apidoc',
        # output dir
        '-o', autodoc_output,
        "--implicit-namespaces",
        # source dir
        str(install_path),
    )

    # generate html
    html_output = "out/_html"
    if os.path.exists(html_output):
        shutil.rmtree(html_output)
    session.run(
        "sphinx-build", "docsource", html_output,
        env={
            "WEB_SERVICE_FQDN": web_service_fqdn,
            "WEB_SERVICE_PORT": web_service_port
        }
    )


@nox.session(python=python, venv_backend="conda")
def build(session: Session) -> None:
    args = session.posargs or src_locations
    session.install("build")

    for src_dir in args:
        namespace_dir = str(Path(src_dir).parent)
        session.run("python", "-m", "build", "--outdir", "dist", namespace_dir)


@nox.session(python=python, venv_backend="conda")
def clean(session: Session) -> None:
    dirs_to_remove = ['out', 'dist', 'build', ".eggs",
                      '.pytest_cache', 'docsource/api', '.mypy_cache']
    egg_info = glob.glob("jobmon_*/src/*.egg-info")
    dirs_to_remove.extend(egg_info)
    builds = glob.glob("jobmon_*/build")
    dirs_to_remove.extend(builds)

    for path in dirs_to_remove:
        if os.path.exists(path):
            shutil.rmtree(path)

    files_to_remove = ['test_report.xml', '.coverage']
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)


@nox.session(python=python, venv_backend="conda")
def launch_gui_test_server(session: Session) -> None:
    session.conda_install("mysqlclient")
    if os.path.exists("/tmp/tests.sqlite"):
        os.remove("/tmp/tests.sqlite")
    session.install("-e", "./jobmon_core")
    session.install("-e", "./jobmon_client")
    session.install("-e", "./jobmon_server")
    session.run("python", "jobmon_gui/local_testing/jobmon_gui/testing_servers/_create_sqlite_db.py")
