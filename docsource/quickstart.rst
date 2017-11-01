Quickstart
##########

.. todo::
    Add a 'configure' subcommand to jobmon cli for initial rcfile setup
.. todo::
    Add a 'test' subcommand to jobmon cli to ensure initial setup was run
    properly


To get started::

    pip install jobmon
    <<jobmon configure>>
    <<jobmon test>>

The primary means of interacting with Jobmon is through a TaskDag. The TaskDag
allows you to create Tasks and specify their dependencies (on other Tasks).
This specification of Tasks and dependencies takes the shape of a
directed-acyclic-graph (or DAG). Example::

    include code sample


(Stick this in a different, lower-level interactions section). Here's a code
sample using the low-level JobListManager for job control::

    include code sample


.. note::

    Jobmon is intended for usage on the SGE cluster. At present, it has limited
    capabilities for executing jobs locally on a single machine using either
    sequential or Multiprocessing. These local job-management capabilities will
    be improved going forward, but SGE support will always be the primary goal
    for the project.

Jobmon Database
***************

By default, your Dag/JobListManager talks to our centrally-hosted jobmon
server (jobmon-p01.ihme.washington.edu). You can access the database
using (need read-only credentials).

.. todo:: Create READ-ONLY credentials
