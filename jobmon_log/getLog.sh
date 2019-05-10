#!/bin/bash

#This rotates each log, so <jobmon>.log.2 becomes <jobmon>.log.3 etc
#It then saves the last 24 hours as <jobmon>.log.0
#Notice that the current oldest log <jobmon>.log.7 is lost.

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

