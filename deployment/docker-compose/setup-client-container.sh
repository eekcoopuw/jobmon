#!/bin/bash

set -e

>&2 echo "client started"
>&2 pip list
i="0"

while [ $i -lt 1 ]
do
    sleep 30
done
>&2 echo "client done"