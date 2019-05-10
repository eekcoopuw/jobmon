#!/bin/bash

dir=$1
jobmons=`docker ps |awk '{print $12}' |grep . |grep _jobmon_`
for j in $jobmons; do
    echo $j
    for i in {1..7}; do
        if test -f "$1/$j.log.$((7-i))"; then
            echo "cat $1/$j.log.$((7-i)) > $1/$j.log.$((8-i))"
            cat $1/$j.log.$((7-i)) > $1/$j.log.$((8-i))
        fi
    done
    echo "docker logs $j --since 24h >  $1/$j.log.0"
    docker logs $j --since 24h >  $1/$j.log.0
done

