"""Nox Configuration for Jobmon."""
import argparse
import os
from subprocess import Popen, PIPE
import shutil

import nox
from nox.sessions import Session


src_locations = ["src/jobmon"]
test_locations = ["tests"]

python = "3.7"


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
    # "darglint",
    # "flake8-bandit"
    session.install("flake8",
                    "flake8-annotations",
                    "flake8-import-order",
                    "flake8-docstrings",
                    "flake8-black")
    session.run("flake8", *args)


@nox.session(python="3.7", venv_backend="conda")
def black(session):
    args = session.posargs or src_locations + test_locations
    session.install("black")
    session.run("black", *args)


@nox.session(python=python, venv_backend="conda")
def typecheck(session: Session) -> None:
    """Type check code."""
    args = session.posargs or src_locations
    session.install("-e", ".")
    session.install("mypy", "types-Flask", "types-requests", "types-PyMySQL", "types-filelock",
                    "types-PyYAML", "types-setuptools", "types-tabulate")
    session.run("mypy", *args)


@nox.session(python=python, venv_backend="conda")
def docs(session: Session) -> None:
    """Build the documentation."""

    # environment variables used in build script
    web_service_fqdn = \
        os.environ.get("WEB_SERVICE_FQDN") if "WEB_SERVICE_FQDN" in os.environ else "TBD"
    web_service_port = \
        os.environ.get("WEB_SERVICE_PORT") if "WEB_SERVICE_PORT" in os.environ else "TBD"

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
    session.run("sphinx-build", "docsource", "out/_html",
        env={
            "WEB_SERVICE_FQDN": web_service_fqdn,
            "WEB_SERVICE_PORT": web_service_port
        }
    )


@nox.session(python=python, venv_backend="conda")
def distribute(session: Session) -> None:
    session.run("python", "setup.py", "sdist", "bdist_wheel")


@nox.session(python=python, venv_backend="conda")
def conda_build(session: Session) -> None:
    session.conda_install("conda-build", "conda-verify")

    # environment variables used in meta.yaml
    pypi_url = os.getenv(
        "PYPI_URL", "https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
    )
    conda_client_version = os.getenv("CONDA_CLIENT_VERSION", "0.0")
    jobmon_version = os.getenv("JOBMON_VERSION", "2.2.2.dev448")
    jobmon_uge_version = os.getenv("JOBMON_UGE_VERSION", "0.1.dev53")
    slurm_rest_version = os.getenv("SLURM_REST_VERSION", "1.0.0")
    jobmon_slurm_version = os.getenv("JOBMON_SLURM_VERSION", "0.1.dev57")
    jenkins_build_number = os.getenv('JENKINS_BUILD_NUMBER', 0)

    # environment variables used in build script
    web_service_fqdn = os.environ["WEB_SERVICE_FQDN"]
    web_service_port = os.environ["WEB_SERVICE_PORT"]

    # paths
    repo_dir = os.path.dirname(__file__)
    recipe_dir = os.path.join(repo_dir, "deployment", "conda_recipe", "ihme_client")
    output_dir = os.path.join(repo_dir, "conda_build_output")

    session.run(
        "conda", "build", recipe_dir,  # build this recope
        "-c", "conda-forge",  # pull build dependencies from conda-forge
        "--no-anaconda-upload",  # don't upload
        "--verify",  # verify build
        "--output-folder", output_dir,  # store build artifacts relative to repo root
        env={
            "PYPI_URL": pypi_url,
            "CONDA_CLIENT_VERSION": conda_client_version,
            "JOBMON_VERSION": jobmon_version,
            "JOBMON_UGE_VERSION": jobmon_uge_version,
            "SLURM_REST_VERSION": slurm_rest_version,
            "JOBMON_SLURM_VERSION": jobmon_slurm_version,
            "JENKINS_BUILD_NUMBER": jenkins_build_number,
            "WEB_SERVICE_FQDN": web_service_fqdn,  # eg. 10.158.146.73
            "WEB_SERVICE_PORT": web_service_port
        }
    )


@nox.session(python=python, venv_backend="conda")
def clean(session: Session) -> None:
    dirs_to_remove = ['out', 'jobmon_coverage_html_report', 'dist', 'build', '.eggs',
                      '.pytest_cache', 'docsource/api', '.mypy_cache', 'conda_build_output']
    for path in dirs_to_remove:
        if os.path.exists(path):
            shutil.rmtree(path)

    files_to_remove = ['test_report.xml', '.coverage']
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)
