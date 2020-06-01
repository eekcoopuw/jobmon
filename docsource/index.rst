.. jobmon documentation master file, created by
   sphinx-quickstart on Fri Sep 23 09:01:26 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Jobmon
######

The jobmon package intends to provide simple, central monitoring of statuses
and errors encountered by distributed tasks. It seeks to easily drop-in
to existing code bases without significant refactoring.

In addition to simple monitoring, it provides utilities for managing a graph of
interrelated jobs. It can manage the sequencing of job execution based on the
interdependencies you declare and can retry jobs that might fail due to random
cluster instability.


Table of Contents
*****************

.. toctree::
    :maxdepth: 4

    quickstart
    advanced_dependencies
    dag_visualization
    services
    tests
    azure
    glossary
    design_notes
    API Reference <modules>
    future_work
    architecture


Indices and tables
******************

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
