FROM buildpack-deps:stretch
ARG SDIST
RUN mkdir /app/
COPY dist/$SDIST /app/
COPY requirements.txt /app/
COPY tests/ /app/tests/
WORKDIR /app/
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		r-base \
		python3.5 \
		python3.5-dev \
		python3-pip \
		python3-setuptools \
		r-bioc-limma
RUN pip3 install wheel
RUN pip3 install $SDIST gunicorn
