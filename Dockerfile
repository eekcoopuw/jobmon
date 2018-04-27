FROM python:3.6
RUN apt-get update && apt-get install -y mysql-client

# PIPS
COPY . /app
WORKDIR /app
RUN cp /app/jobmonrc-docker-wsecrets $HOME/.jobmonrc
RUN pip install cluster_utils --extra-index-url http://dev-tomflem.ihme.washington.edu/simple --trusted-host dev-tomflem.ihme.washington.edu
RUN pip install .
