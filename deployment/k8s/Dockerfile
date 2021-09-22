FROM tiangolo/uwsgi-nginx-flask:python3.8
RUN apt-get update && apt-get install -y mariadb-client

ENV NGINX_WORKER_PROCESSES auto
ENV UWSGI_INI /app/uwsgi.ini

# jobmon packaging and dependencies
COPY ./requirements.txt /app/requirements.txt

# server configs
COPY ./deployment/config/app/prestart.sh /app/prestart.sh
COPY ./deployment/config/app/nginx_app.conf /app/nginx_app.conf
COPY ./deployment/config/app/uwsgi.ini /app/uwsgi.ini

RUN chmod +x /app/prestart.sh
WORKDIR /app

# PIPS
RUN pip install --upgrade pip
RUN pip install mysqlclient
RUN pip install -r /app/requirements.txt --extra-index-url https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
