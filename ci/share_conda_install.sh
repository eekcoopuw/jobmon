#!/bin/bash

#this parameter comes from Jenkins
env_base=$1
jobmon_version=$2
conda_root=$3

env_name=$env_base/$jobmon_version
minor_version=$(echo $jobmon_version |awk -F"." '{print $1"."$2}')
export PATH=$PATH:$conda_root/bin
eval "$(conda shell.bash hook)"

echo "Creating environment $env_name"
umask 002
conda create -y --name="$env_name" python=3.9
# check status of previous command
if [ $? -eq 0 ]; then
    echo "Created environment ${env_name}"
else
    echo "Creating conda env failed"
    exit 1
fi
echo "Activating environment $env_name"
source activate "$env_name"
# check status of previous command
if [ $? -eq 0 ]; then
    echo "Activated environment ${env_name}"
else
    echo "Activating conda env failed"
    exit 1
fi
echo "Installing jobmon for environment $env_name" &&
pip install jobmon==$jobmon_version &&
echo "linking jobmon $env_name to  && $minor_version"
eval "$(rm $env_base/$minor_version ||true)" &&
eval "$(ln -s $env_base/$jobmon_version $env_base/$minor_version)