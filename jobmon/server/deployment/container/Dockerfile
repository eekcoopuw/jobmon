FROM tiangolo/uwsgi-nginx-flask:python3.6
RUN apt-get update && apt-get install -y mysql-client

ENV NGINX_WORKER_PROCESSES auto

# move app files
COPY ./jobmon /app/jobmon
COPY ./jobmon/server/deployment/container/setup.cfg /app/setup.cfg
COPY ./jobmon/server/deployment/container/setup.py /app/setup.py
COPY ./jobmon/server/deployment/container/autowrap.cfg  /app/autowrap.cfg
COPY ./jobmon/server/deployment/container/versioneer.py  /app/versioneer.py
COPY ./jobmon/server/deployment/container/wait-for-db.sh  /app/wait-for-db.sh
COPY ./jobmon/server/deployment/container/wait-for-tables.sh  /app/wait-for-tables.sh
WORKDIR /app

# PIPS
RUN pip install cluster_utils --extra-index-url http://dev-tomflem.ihme.washington.edu/simple --trusted-host dev-tomflem.ihme.washington.edu
RUN pip install flask-cors
RUN pip install .
