
Logical View (aka software layers, Component View)
**************************************************

TODO Needs Major Revision or Removal. Perhaps just a reference back to COre
components? Or is this software layers?

What is a Component?

Components are mini-products. Control and responsibility are their defining characteristics.

In the source control system a component is one directory tree.
It contains every kind of code needed for that component: Python, sql, javascript, etc.

Suppose we needed to add authentication and authorisation to the rate limiting feature in jobmon.
For this example, also assume that we could not find an existing external system for people,
organizations, and their relationships.
Therefore we need to construct an Organization component that is completely responsible for that area.
It will have uses cases for:

- CRUD a user (full CRUD)
- CRUD a team (full CRUD)
- CRUD an application
- Get escalation path for a user
- Is user authorized to control this application?

CRUD = Create, Read, Update, Delete of a Domain Object.

It needs code at the following layers:

- HTML and Javascript for the CRUD screens
- Python API and then code  for validating CRUD screens, computing escalation paths, authentication etc
- Database tables

The different kinds of code are deployed in different places.
Organize the source tree by the are of responsibility, it makes it easier for a maintenance programmer

FYI CRUD = Create, Read, Update, Delete.

*In hindsight I think the following is a little Hyper-modern: abstractly appealing, but too fiddly in practise.*
Systems rarely need to be so modular that new ones can be
composed from arbitrary subsets.

In practise each deployment unit has its own source tree.
The code would be clearer if the relevant fragment of each Domain Object was
clearly identified in each deployment unit.
Jobmon is probably one component in its own right, as is
QPID, UGE, and the organizational component described below.


Components in Guppy
===================

The Python packages are currently organized according to the deployment architecture,
not by the major noun, although each deployment unit specializes in certain Domain Objects.

Perhaps components make sense within a deployment unit,
and this section should be repeated within each of the three deployment groups.
