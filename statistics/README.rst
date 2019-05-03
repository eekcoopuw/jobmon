This directory currently contains some Python scripts that can be manually
run to generate simple usage statistics for Jobmon.

Error_rates.py creates the H5 files that jupyter notebook needs.
The jupyter notebook produces metrics and graphs for the CSC paper.

It only had to run once, in March 2019 for the CSC paper.
Therefore this code is sketchy and won't run again without work.
The databases will probably have been moved to dev-tomflem.
If the paper is accepted then this code should be revived,
although this will never need to be production quality.

Perhaps this could be the basis of KPI statistics generator.

The passwords have been removed.

The database containers have to be spun up manually on the docker host,
see dbup.sh
