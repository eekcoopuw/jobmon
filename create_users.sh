#!/bin/bash

SQL="
CREATE USER 'table_creator'@'%' IDENTIFIED BY '${JOBMON_PASS_TABLE_CREATOR}'; \
CREATE USER 'service_user'@'%' IDENTIFIED BY '${JOBMON_PASS_SERVICE_USER}'; \
CREATE USER 'read_only'@'%' IDENTIFIED BY '${JOBMON_PASS_READ_ONLY}'; \
FLUSH PRIVILEGES; \
GRANT CREATE, INSERT, SELECT, REFERENCES ON docker.* TO 'table_creator'@'%'; \
GRANT INSERT, SELECT, UPDATE ON docker.* TO 'service_user'@'%'; \
GRANT SELECT ON docker.* TO 'read_only'@'%'; \
FLUSH PRIVILEGES;"

mysql -u root --password=${JOBMON_PASS_ROOT} -e"$SQL"
