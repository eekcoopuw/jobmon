FROM tiangolo/uwsgi-nginx-flask:python3.7
RUN apt-get update && apt-get install -y mariadb-client

ENV NGINX_WORKER_PROCESSES auto

# move app files
COPY ./jobmon /app/jobmon
COPY ./uwsgi.ini /app/uwsgi.ini
COPY ./setup.cfg /app/setup.cfg
COPY ./setup.py /app/setup.py
COPY ./versioneer.py  /app/versioneer.py
COPY ./jobmon/server/deployment/container/wait-for-tables.sh  /app/wait-for-tables.sh
RUN chmod +x /app/setup.py /app/versioneer.py /app/wait-for-tables.sh
WORKDIR /app

# PIPS
RUN pip install cluster_utils --extra-index-url https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple --trusted-host artifactory.ihme.washington.edu
RUN pip install flask-cors
RUN pip install .
