FROM tiangolo/uwsgi-nginx-flask:python3.6
RUN apt-get update && apt-get install -y mysql-client

ENV NGINX_WORKER_PROCESSES auto

# move app files
COPY ./jobmon /app/jobmon
COPY ./jobmon/uwsgi.ini /app/uwsgi.ini
COPY ./setup.cfg /app/setup.cfg
COPY ./setup.py /app/setup.py
COPY ./versioneer.py  /app/versioneer.py
COPY ./jobmon/server/deployment/container/wait-for-db.sh  /app/wait-for-db.sh
COPY ./jobmon/server/deployment/container/wait-for-tables.sh  /app/wait-for-tables.sh
RUN chmod +x /app/setup.py /app/versioneer.py /app/wait-for-db.sh /app/wait-for-tables.sh
WORKDIR /app

# PIPS
RUN pip install cluster_utils --extra-index-url http://dev-tomflem.ihme.washington.edu/simple --trusted-host dev-tomflem.ihme.washington.edu
RUN pip install flask-cors
RUN pip install .
