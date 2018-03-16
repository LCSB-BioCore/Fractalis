FROM python:3.6-stretch
ARG SDIST
COPY $SDIST .
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
	    r-bioc-limma \
    && pip3 install wheel gunicorn \
    && pip3 install fractalis-*.tar.gz \
    && rm fractalis-*.tar.gz \
    && rm -rf /var/lib/apt/lists/* \
    && find /usr/local -type d -name '__pycache__'  -exec rm -rf {} +
