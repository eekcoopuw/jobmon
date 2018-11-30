#!/usr/bin/env bash

conda create -y -n my_jobmon_env python=3.7
source activate my_jobmon_env
conda install -y --file conda_requirements.txt
pip install -r requirements.txt
pip install -e .

# one of the (conda_)requirements.txt deps installs a different, breaking
# version of openssl
conda install -y -c conda-forge openssl
