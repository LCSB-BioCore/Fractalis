FROM buildpack-deps:stretch

RUN mkdir /app
COPY . /app
WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		r-base \
		python3.5 \
		python3.5-dev \
		python3-pip \
		python3-setuptools \
		r-bioc-limma

RUN pip3 install .
RUN pip3 install gunicorn
