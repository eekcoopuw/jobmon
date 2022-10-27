
Constraints and Non-functional Requirements
*******************************************

Scaling
=======

The goal will be to run "all jobs" on the cluster.
The current largest workflow is the Burdenator, with about 500k jobs.
Application Operators have twice submitted workflows with about 1.5 million tasks,
although they are arguably over-parallelized.
On IHME's cluster Jobmon should plan for 20% annual growth in all dimensions.

+-----------+-----------------------+---------------------------+---------------------------+
| Date      |	Largest workflow    | Simultaneous workflows    | Transactions per Second   |
+===========+=======================+===========================+===========================+
| Jan 2021  |	500k                |                           |                           |
+-----------+-----------------------+---------------------------+---------------------------+
| June 2021 |	1 million           |                           |                           |
+-----------+-----------------------+---------------------------+---------------------------+

Performance numbers need to more carefully recorded.

Security
========
Security does not have to be especially high because Jobmon only has metadat on jobs.
However, it must not be possible to use
Jobmon to launch bad-actor jobs on the cluster. For example, exposing a service to the internet
that allows an external Jobmon to run jobs on the cluster would be a big security risk.
Jobmon relies on existing IHME security systems.

Jobmon stores no data apart from commands, so the cost of
a data breach would be low.

Lifetime Maintainability
========================
Plan for a 5-10 year lifetime

Portability
===========
Jobmon was designed and developed as a sequence of Minimal Viable Product releases, so it was not
designed to be a cross-platform system. However, it is highly portable because it only depends
on Python, web technologies, sql, and the cluster OS is abstracted behind the Executor API.

MPI support could be difficult.

GPUs can be supported if they are implemented in separate queues in the cluster OS.

Usability
=========

Usability is key, otherwise Jobmon will not be adopted.
It must be easier than raw UGE, preferably easier than Azure Bath Service and SLURM.
However, we have no experience with SLURM and it might not have the usability problems
present in UGE. Specifically:

Retries: UGE has one global setting for the number of retries, Jobmon allows the number of retires to be set per task.

