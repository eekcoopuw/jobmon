#!/bin/bash
# This file activates the conda environment
tt=/tmp/env_$$
echo "Hi $$" > $tt
echo "PWD " `pwd` >> $t
echo "$*" >> $tt
export PATH=$1:$PATH
source activate $2 >> $tt 2>&1
shift 2
"$@" >> $tt 2>&1
