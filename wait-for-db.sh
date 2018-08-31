#!/bin/bash

# See: https://docs.docker.com/compose/startup-order/
# wait-for-db.sh

set -e

host="$1"
shift
cmd="$@"

until mysql -e 'SHOW TABLES;' --host=$host --user=table_creator --password=$JOBMON_PASS_TABLE_CREATOR docker; do
  >&2 echo "DB is unavailable - sleeping"
  sleep 3
done

>&2 echo "DB is up - executing command"
$cmd
