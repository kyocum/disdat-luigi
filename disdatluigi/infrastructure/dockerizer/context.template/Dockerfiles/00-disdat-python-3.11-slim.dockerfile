#
# Kickstart the Python 3.11 system environment
#

FROM python:3.11-slim

# Kickstart shell scripts root
ARG KICKSTART_ROOT
ENV KICKSTART_ROOT $KICKSTART_ROOT

RUN apt-get update \
    && apt-get upgrade -y

# Install git and a minimal Python 3.x toolchain. Disdat uses git to detect
# changed sources when deciding whether or not to rerun a pipeline.
# disdat uses pyodbc which requires gcc ,hence 'build-essential'
# sometimes people need to install .deb files, hence gdebi
RUN apt-get install -y git build-essential unixodbc-dev \
    && pip install --no-cache-dir virtualenv==20.24.6

# Since Ubuntu/Debian newer images may default to OpenSSL security level 2,
# relax to level 1 to preserve compatibility with some servers.
# NOTE: Ideally, this should be fixed server-side; this is a client-side workaround.
RUN if [ -f /etc/ssl/openssl.cnf ]; then \
      mv /etc/ssl/openssl.cnf /etc/ssl/openssl.back.cnf && \
      sed -e 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.back.cnf > /etc/ssl/openssl.cnf; \
    fi

# Install the kickstart scripts used by later layers
COPY kickstart $KICKSTART_ROOT

# Declare Miniconda configurable arguments. We only need to save the Python
# virtual environment path for later stages.
ARG VIRTUAL_ENV
ENV VIRTUAL_ENV $VIRTUAL_ENV
