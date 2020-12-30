FROM tiangolo/uwsgi-nginx-flask:python3.7
RUN apt-get update && apt-get install -y mariadb-client

ENV NGINX_WORKER_PROCESSES auto
ENV UWSGI_INI /app/uwsgi.ini

# jobmon packaging
COPY ./jobmon /app/jobmon
COPY ./setup.cfg /app/setup.cfg
COPY ./setup.py /app/setup.py
COPY ./README.md /app/README.md
COPY ./LICENSE /app/LICENSE
COPY ./MANIFEST.in /app/MANIFEST.in

# Server setup
COPY ./deployment/config/app/nginx_app.conf /etc/nginx/conf.d/nginx.conf
COPY ./deployment/config/app/uwsgi.ini /app/uwsgi.ini
COPY ./deployment/docker-compose/wait-for-tables.sh  /app/wait-for-tables.sh
COPY ./deployment/docker-compose/setup-client-container.sh /app/setup-client-container.sh

RUN chmod +x /app/setup.py /app/wait-for-tables.sh /app/setup-client-container.sh
WORKDIR /app

# PIPS
RUN pip install flask-cors
RUN pip install .