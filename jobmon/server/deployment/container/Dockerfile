FROM tiangolo/uwsgi-nginx-flask:python3.7
RUN apt-get update && apt-get install -y mariadb-client

ENV NGINX_WORKER_PROCESSES auto

# jobmon packaging
COPY ./jobmon /app/jobmon
COPY ./setup.cfg /app/setup.cfg
COPY ./setup.py /app/setup.py
COPY ./versioneer.py  /app/versioneer.py

# server configs
COPY ./jobmon/server/deployment/container/prestart.sh /app/prestart.sh
COPY ./jobmon/server/deployment/container/nginx_app.conf /app/nginx_app.conf
COPY ./jobmon/server/deployment/container/uwsgi.ini /app/uwsgi.ini
COPY ./jobmon/server/deployment/container/wait-for-tables.sh  /app/wait-for-tables.sh

RUN chmod +x /app/setup.py /app/versioneer.py /app/wait-for-tables.sh /app/prestart.sh
WORKDIR /app

# PIPS
RUN pip install cluster_utils --extra-index-url https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple --trusted-host artifactory.ihme.washington.edu
RUN pip install flask-cors
RUN pip install .
