#!/bin/bash

# See: https://docs.docker.com/compose/startup-order/
# wait-for-db.sh

set -e

host="$1"
shift
cmd="$@"

NUM_TABLES=0
until [[ $NUM_TABLES -gt 0 ]]; do
    NUM_TABLES=$(mysql -e 'SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_SCHEMA="docker";' --host=$host --port=3312 --user=docker --password=docker docker | sed -n 2p)
    >&2 echo "$NUM_TABLES tables. DB is unavailable - sleeping"
    sleep 3
done

>&2 echo "DB is up - executing command"
$cmd
