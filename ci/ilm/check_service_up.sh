#!/bin/sh

#check if server is up
num=0
curl $1
while [  $? -ne 0 ] && (( $num<100 )); do
   sleep 10
   num=$((num+1))
   curl $1
done;

#check if api is ready
num=0
r=$(curl -LI $1 -o /dev/null -w '%{http_code}\n' -s)
echo $r
while [ "$r" != "200" ] &&  (( $num<100 )); do
    sleep 10
    num=$((num+1))
    r=$(curl -LI $1 -o /dev/null -w '%{http_code}\n' -s)
done;

if (( $num==100 )); then
    echo "Server failed to start"
fi
