*********************
Requirements Analysis
*********************

Roles
=====

A Role (aka Actor) is a human or an external system that interacts with Jobmon.
Most roles are human, but some system roles exist because they initiate a use case.
For example, the Slurm scheduler is a system role because it initiates the Use Case "Launch a Job."

One person will often play the part of different Roles during the same day.
For example, at IHME a Coder will often also be an Application Operator or an Observer.
Therefore Roles are not job titles.

Technically, a Role is a Domain Object that can initiate a Use Case.

Human Roles
===========

Observer
  Typically a TPM or a Project Officer.
  An Observer is not a modeller or coder but they do understand the phases and general running of the Tool.
  For example, "Has location aggregation started yet?" is something they might ask.

  A person who is interested in how the workflow is going, and especially when it will be done, is it running smoothly.
  For example, Project Officer of the owning team or a downstream team. Would only have read-only access
  Distant Observer

  The classic example is a Principal Investigator (PI). They can click on a link that is emailed to them.
  They don't have the time or knowledge to run a complex search to find what they are interested in.
  They do not understand internal structure of the modeling pipeline.
  Only interested in "Are we there yet?" and "How long will it be?"

Operator
  Someone who starts, stops, observers, and resumes workflows, finds errors.
  Cannot assume that they know how the code is written, but they are very familiar with what it does. Data Analysts and
  Research Engineers are typical operators.

Implementer
  Someone who is implementing a research pipeline.
  They probably only operate the pipeline during tests, and maybe only from the command line. Data Analysts, Research Engineers, and Software engineers spend most of their time as Implementers.

Debugger
  Someone who is implementing a research pipeline.
  This role is deliberately separate to the Implementor to emphasize that pipelines are often debugged by people who did not write that pipeline and don't have intimate knowledge. Data Analysts, Research Engineers, and Software engineers are often Debuggers

Prototyper
  A special type of implementer-Operator who just wants to "get it done."
  One run only for this paper deadline, hoping to never run the code again (although it often is  run again next year)
  A typical Data Analyst activity.


System Roles
============

- Python Control Script
- R Control Script
- UGE Distributor (it starts jobs)
- cgroups (it kills jobs)
- OOM Killer (it also kills jobs if cgroups failes)
- Cluster Distributor (Broadly UGE, Azure, SLURM)
- The Gremlin (a synthetic System Role, it causes hardware to fail)

Domain Objects
==============

Any noun mentioned in a use case must either be a role or a domain object.
Domain Objects are typically capitalized to show that they are defined terms.
Domain objects might not be implemented in code. For example, Jobmon originally
had no "cluster" object, although Jobmon could need it when it has multiple executors.
A domain object might be implemented by different pieces of code, depending on its
location in the deployment architecture. For example, domain objects such as Workflow
are implemented in the database schema, the sqlalchemy model objects, as a wire format,
and as a stub in the Python client and the experimental R client.

All domain objects are defined in the :doc:`../glossary`

Domain Objects that are mentioned in the Use Cases but are not part of Jobmon:

- Clurm job
- Linux Process
- Conda environment
- Cluster Node
- Python Application
- R Application

Use Cases
=========
Use Cases all follow the naming pattern:

*<Role> <Verb> <Domain-Object Phrase>*

For example:

- UGE Launches Job
- Python-Application Creates Workflow
- Python-Application Runs Workflow
- Gremlin breaks a Cluster Node


In a waterfall project this Use Case section would be much bigger. Jobmon was developed using
the agile process, therefore the requirements were defined along the way.
The use cases identified here are looking forward to an operating GUI, and as examples.


Coder Use Cases
===============

100. Coder Converts a direct Cluster Job Launching Script to Jobmon

Included to emphasize the importance of usability, this use case will describe the extra steps that are necessary


Application Operator Use Cases
==============================

210. Application Operator Starts Application

220. Observer Monitors Application

They ask questions like: *How is it going? Are there any Failures? When will it be done?*
Originally they had to run queries in the database. Now they can use a CLI.
A GUI would open up this feature to more Application Owners.

230. Operator Debugs Application

How do they find the task statuses? How do they find Errors from their own applications?

Observer Use Cases
==================

220. Observer Monitors Application

They ask questions like: *How is it going? Are there any Failures? When will it be done?*
Originally they had to run queries in the database. Now they can use a CLI.
A GUI would open up this feature to more Application Owners.


Jobmon Distributor Use Cases
============================

330. Jobmon submits a Job to UGE

This is a key use case. It must show the flow from the control node to UGE and the special
flags to qsub command needed for the environment.

UGE Use Cases
=============

410. UGE Job starts

Discuss the
# initial bash script
# the python execution wrapper
# Call-backs to central services to show progress
# Launching the actual application code in a sub-process
# Need for careful exception handling


420. UGE Job finishes, with or without error

430. Cgroups kills a UGE for excess Resource Usage

