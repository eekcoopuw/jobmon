FROM ubuntu:14.04
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    htop \
    mysql-client \
    vim \
&& rm -rf /var/lib/apt/lists/*

# INSTALL MINICONDA
RUN curl -LO https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN bash Miniconda3-latest-Linux-x86_64.sh -p /miniconda -b
RUN rm Miniconda3-latest-Linux-x86_64.sh
ENV PATH=/miniconda/bin:${PATH}
RUN conda update -y conda

# COPY ODBC DEFS
COPY pip.conf /root/.pip/pip.conf

# PIPS
COPY . /app
WORKDIR /app
RUN pip install --requirement requirements.txt
RUN pip install .
