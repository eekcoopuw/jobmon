#!/bin/sh
#$ -S /bin/sh
export PATH=$1:$PATH
shift
"$@"
