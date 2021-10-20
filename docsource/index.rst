.. jobmon documentation master file, created by
   sphinx-quickstart on Fri Sep 23 09:01:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Jobmon
######

Jobmon is a Scientific Workflow Management system developed at IHME specifically for the
institute's needs. Jobmon aims to reduce human pain by providing:

- An easy to use Python API that matches existing code patterns.
- Centralized monitoring of jobs, including the job's status and errors.
- Automatic retries to protect against random cluster failures.
- Automatic retries following a resource failures, e.g. re-running a job with increased memory.
- Whole-of-application resumes to handle missing data or in-flight code fixes.
- Fine-grained job dependencies.



Table of Contents
*****************

.. toctree::
    :maxdepth: 2

    quickstart
    glossary
    advanced_dependencies
    architecture
    databases
    kubernetes_deployment
    docker_deployment
    tests
    API Reference <api/modules>
    experimental_azure_kubernetes_deployment
    design/finite_state_machine


Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
