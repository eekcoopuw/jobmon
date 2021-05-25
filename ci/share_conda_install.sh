#!/bin/bash

#this parameter comes from Jenkins
env_base=$1
jobmon_version=$2
conda_root=$3

minor_version=$(echo $jobmon_version |awk -F"." '{print $1"-"$2}')
env_name=$env_base/jobmon_$(echo $jobmon_version | tr "." "-")

export PATH=$PATH:$conda_root/bin
eval "$(conda shell.bash hook)"

echo "Creating environment $env_name"
umask 002
conda create -y --prefix="$env_name" python=3.9
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
echo "linking jobmon $env_name to  jobmon_$minor_version"
eval "$(rm -rf $env_base/jobmon_$minor_version 2>/dev/null ||true)" # silent
eval "$(ln -s $env_name $env_base/jobmon_$minor_version)"