#!/bin/bash
# This file activates the conda environment

export PATH=$1:$PATH
source activate $2
shift 2
"$@"
