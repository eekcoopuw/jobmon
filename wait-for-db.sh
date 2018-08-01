#!/bin/bash

# See: https://docs.docker.com/compose/startup-order/
# wait-for-db.sh

set -e

host="$1"
shift
cmd="$@"

until mysql -e 'SHOW TABLES;' --host=$host --port=3312 --user=docker --password=docker docker; do
  >&2 echo "DB is unavailable - sleeping"
  sleep 3
done

>&2 echo "DB is up - executing command"
$cmd
