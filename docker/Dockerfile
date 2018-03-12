FROM python:3.6-stretch
ARG SDIST
RUN mkdir /app/
COPY $SDIST /app/
COPY requirements.txt /app/
COPY tests/ /app/tests/
WORKDIR /app/
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		r-bioc-limma
RUN pip3 install wheel
RUN pip3 install fractalis-*.tar.gz gunicorn
