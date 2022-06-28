FROM ubuntu:20.04


# Install system librarires for Python packages:
# * psycopg2
RUN apt-get update && \
    apt-get install --no-install-recommends --yes \
        libpq-dev gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*

# install graph-tool for postprocessing
RUN apt-get update
RUN apt-get install -qy apt-utils
RUN apt-get -qy install software-properties-common
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-key 612DEFB798507F25
RUN add-apt-repository 'deb [ arch=amd64 ] https://downloads.skewed.de/apt focal main'

RUN apt-get update
RUN apt-get install -qy python3-graph-tool

RUN apt-get install -qy python3-pip

# make python3 the default
RUN apt-get install -qy python-is-python3

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Only copy the setup.py, it will still force all install_requires to be installed,
# but find_packages() will find nothing (which is fine). When Docker Compose mounts the real source
# over top of this directory, the .egg-link in site-packages resolves to the mounted directory
# and all package modules are importable.
COPY ./setup.py /opt/django-project/setup.py
RUN pip install --editable /opt/django-project[dev]

# Use a directory name which will never be an import name, as isort considers this as first-party.
WORKDIR /opt/django-project
