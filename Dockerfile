FROM tiangolo/uwsgi-nginx-flask:python3.6
RUN apt-get update && apt-get install -y mysql-client

ENV NGINX_WORKER_PROCESSES auto

# move app files
COPY . /app
WORKDIR /app

# PIPS
RUN pip install cluster_utils --extra-index-url http://dev-tomflem.ihme.washington.edu/simple --trusted-host dev-tomflem.ihme.washington.edu
RUN pip install flask-cors
RUN pip install .
