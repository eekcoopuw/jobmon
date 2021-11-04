.. jobmon documentation master file, created by
   sphinx-quickstart on Fri Sep 23 09:01:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Jobmon
######
TODO: UPDATE THIS SECTION

Jobmon is a Scientific Workflow Management system developed at IHME specifically for the
institute's needs. Jobmon aims to reduce human pain by providing:

- An easy to use Python API that matches existing code patterns.
- Centralized monitoring of jobs, including the job's status and errors.
- Automatic retries to protect against random cluster failures.
- Automatic retries following a resource failures, e.g. re-running a job with increased memory.
- Whole-of-application resumes to handle missing data or in-flight code fixes.
- Fine-grained job dependencies.

#################
Table of Contents
#################

User Manual
===========

.. toctree::
    :maxdepth: 2
    :caption: User Manual

    quickstart
    core_concepts
    monitoring_debugging
    advanced_usage
    glossary
    API Reference <api/modules>

Developer's Guide
=================

.. toctree::
   :maxdepth: 2
   :caption: Developer's Guide

   developers_guide/index


Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
