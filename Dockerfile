FROM python:3.6
RUN apt-get update && apt-get install -y mysql-client

# PIPS
COPY . /app
WORKDIR /app
RUN cp /app/jobmonrc-docker $HOME/.jobmonrc
RUN pip install .
