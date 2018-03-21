FROM python:3.6-stretch
RUN mkdir /app/

COPY requirements.txt /app/
COPY tests/ /app/tests/

WORKDIR /app/
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		r-bioc-limma \
    && pip3 install wheel

ARG SDIST
COPY $SDIST /app/
RUN pip3 install fractalis-*.tar.gz gunicorn
