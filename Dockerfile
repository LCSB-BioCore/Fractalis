FROM buildpack-deps:stretch

WORKDIR /app
COPY . /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		r-base \
		python3.5 \
		python3.5-dev \
		python3-pip \
		python3-setuptools \
		rabbitmq-server \
		redis-server \
		r-bioc-limma
RUN pip3 install wheel
RUN pip3 install -e /app --default-timeout 180
RUN /etc/init.d/redis-server start
RUN /etc/init.d/rabbitmq-server start
RUN celery worker -A fractalis:celery -D -l info

EXPOSE 5000
CMD ["python3", "fractalis/__init__.py"]
