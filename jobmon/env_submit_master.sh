#!/bin/bash
# This file activates the conda environment

# The while-sleep loop is a work-around for a bug whereby parallel conda source activate calls stomp on each other
# by attempting to create each others symlinks.
# see https://github.com/conda/conda/issues/2837

export PATH=$1:$PATH
rc=17  # anything but zero.
tries=0
max_tries=3
while [[ $rc -ne 0 && $tries -lt $max_tries ]]; do
  source activate $2
  rc=$?
  tries=`expr $tries + 1`
  if [ $tries -gt 1 ] ; then
     # Use an ethernet-style increasing backoff
     jitter=`expr $RANDOM % 10 + 1`
     sleep `expr $jitter \* $tries`
     >&2 echo "jobmon.env_submit_master: WARNING source activate failed, retry number: $tries"
  fi
done

if [ $tries -ge $max_tries ] ; then
  >&2 echo "jobmon.env_submit_master: ERROR source activate failed"
  exit
fi

# get rid of the env name
shift 2
"$@"
