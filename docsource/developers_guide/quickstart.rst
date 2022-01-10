************************
Developer Quickstart
************************

The standard workflow for contributing to Jobmon involves:

#. Making your changes, on a feature branch
#. Running the unit tests
#. Linting and type checking the code base
#. Creating a pull request, and ensuring that the automatic builds pass.

Updating code
*************

All code must be version controlled, so the recommended workflow is to:

#. Clone this repository to your machine
#. Create a feature branch, with the name of the ticket in the branch name

    #. Naming the ticket number will link to JIRA, so that the associated branch and any pull requests are
       easily referenced.

#. Make any changes, run the necessary tests, and push to an upstream branch

Running unit tests
****************

To run the Jobmon test suite, navigate to the top-level folder in this repository and run ``nox -s tests -- tests/``.

To reduce the runtime of the tests, you can optionally suffix the above command with ``-n=<number_of_processes>`` to
enable testing in parallel. You can also use ``nox -r ...`` to re-use an existing virtual environment.

nox and pytest
^^^^^^^^^^^^^^

The test suite uses nox to manage virtual testing environments and install the necessary dependencies, and pytest to
define common fixtures like a temporary database and web service. For more details on the unit test architecture, please
refer to the Developer Testing section.

Linting and Typechecking
************************

To run linting and type checking, run ``nox -s lint`` and ``nox -s typecheck`` respectively.

The linting check uses flake8 to check that our code conforms to pep8 formatting standards, with exceptions as defined
in setup.cfg. Type checking uses mypy ensures that our code has the correct type hints and usages conforming to
`PEP484 <https://www.python.org/dev/peps/pep-0484/>`_.

Sometimes the linting check will fail with a message indicating that "Black would make changes". Black is an
autoformatting tool that ensures code conformity. To address this error you can run ``nox -s black``.

Pull Requests
*************

When the above unit tests, formatting checks, and typing checks all pass, you can submit a pull request on Stash. Add
all members of the Scicomp team to the created pull request.

Creating a pull request should start an automatic build, which runs the above mentioned tests and checks on a
provisioned Jenkins server. If all the tests pass, you will see a green check mark on the builds page in your PR.

