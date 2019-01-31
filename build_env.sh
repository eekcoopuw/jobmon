#!/usr/bin/env bash

# Purpose (12-14-2018):
# Typically able to build a working environments by simply specifying
# packages with non-python dependencies in a conda_requirements.txt file and
# other pure python dependencies in a requirements.txt file. However, as of
# Dec 14, 2018, there are issues with the order in which conda and pip
# resolve resolve and install satisfying versions jobmon's dependencies.
# Specifically, the openssl library becomes incompatible when following our
# normal installation sequence. We add a step here to install a working
# version.

conda create -y -n my_jobmon_env python=3.6
source activate my_jobmon_env
conda install -y --file conda_requirements.txt
pip install -r requirements.txt
pip install -e .

# one of the (conda_)requirements.txt deps installs a different, breaking
# version of openssl
conda install -y -c conda-forge openssl
