FROM tiangolo/uwsgi-nginx-flask:python3.7
RUN apt-get update && apt-get install -y mariadb-client

ENV NGINX_WORKER_PROCESSES auto
ENV UWSGI_INI /app/uwsgi.ini

# jobmon packaging
COPY ./jobmon /app/jobmon
COPY ./setup.cfg /app/setup.cfg
COPY ./setup.py /app/setup.py
COPY ./README.md /app/README.md

# Server setup
COPY ./jobmon/server/deployment/container/nginx_app.conf /etc/nginx/conf.d/nginx.conf
COPY ./jobmon/server/deployment/container/uwsgi.ini /app/uwsgi.ini
COPY ./jobmon/server/deployment/container/wait-for-tables.sh  /app/wait-for-tables.sh
COPY ./deployment/docker/setup-client-container.sh /app/setup-client-container.sh

RUN chmod +x /app/setup.py /app/wait-for-tables.sh /app/setup-client-container.sh
WORKDIR /app

# PIPS
RUN pip install flask-cors
RUN pip install .
