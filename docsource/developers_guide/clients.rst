*******
Clients
*******

The Jobmon client is conceptually responsible for basic CRUD (Create, Read, Update, Delete). The client creates and modifies Jobmon
objects like Workflow, Tool, and Task in memory, and adds these objects to the database when the user requests a workflow run.

At the moment of writing, the client also currently includes logic for workflow execution and DAG traversal - however,
this logic will likely eventually be refactored into the web service.

Python Client
^^^^^^^^^^^^^

The core client logic is written in Python, available to use from the ``jobmon`` Python package. A Python user can
install Jobmon into their conda/singularity/etc. environment, and write a control script defining their workflow. The user
will also need to install a plugin package, which acts as an execution interface into the desired cluster they wish to run
on. If installing via conda or using ``pip install jobmon[ihme]``, then all IHME-required plugin packages will be installed
automatically as well (UGE and SLURM).

R Client
^^^^^^^^

R users, who may not have experience writing any Python but still wish to use Jobmon, can use the **jobmonr** library to
write their control scripts. All core logic is still written in Python, but the R client uses the **reticulate** package
to build a simple wrapper around the Python client. This allows a user to create and manipulate the necessary Jobmon
objects without needing to write any Python.

Because there is no self-contained execution logic in the R client, users do need to take a dependency on a Python client.
This is done by setting the RETICULATE_PYTHON environment variable *prior* to loading the jobmonr library. This environment
variable should be a file path to the Python executable within a conda environment that has Jobmon installed, generally
ending with something like ``/miniconda3/bin/python``. If not set by a user, then the R client will default to a centrally
installed conda environment that's managed by the Scicomp team.

**Note**: A key limitation of any reticulate package is that you can only have 1 Python interpreter in memory. This means that 
if you want to use another reticulate package in the same control script, you will need to install all dependencies needed for 
both packages into a single conda environment. Additionally, both packages need to be flexible in allowing the user to set 
the appropriate interpreter. For example, the widely-used (at IHME) crosswalk package, produced by MSCA, has a hardcoded, custom-built
conda environment hardcoded in the startup section, meaning that users *cannot* use both jobmonr and crosswalk in the same memory space. 
However, there is very little reason to do so in the first place, and a small update to the crosswalk package could solve this issue. 

Future Directions
^^^^^^^^^^^^^^^^^

In the long term, the scope of the client should be restricted solely to creation of metadata to send to the database.
DAG traversal and scheduling/distributing would then be performed inside a long-running back end service container.

The decision to use reticulate in the R client was mainly motivated by wanting to avoid the challenge of rewriting all
of the distributor in R. Reticulate provides an extremely simple reference-based interface into a Python program, but
comes with its own set of challenges surrounding type conversions and environment management. If the client is simplified
into pure metadata creation, then we could replace the reticulate interface with a S6-class and httr based approach.
