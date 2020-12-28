FROM tiangolo/uwsgi-nginx-flask:python3.7
RUN apt-get update && apt-get install -y mariadb-client

ENV NGINX_WORKER_PROCESSES auto
ENV UWSGI_INI /app/uwsgi.ini

# jobmon packaging and dependencies
COPY ./jobmon/requirements.txt /app/requirements.txt

# server configs
COPY ./deployment/config/prestart.sh /app/prestart.sh
COPY ./deployment/config/nginx_app.conf /app/nginx_app.conf
COPY ./deployment/config//uwsgi.ini /app/uwsgi.ini

RUN chmod +x /app/prestart.sh
WORKDIR /app

# PIPS
RUN pip install cluster_utils --extra-index-url https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple --trusted-host artifactory.ihme.washington.edu
RUN pip install -r ./requirements.txt
