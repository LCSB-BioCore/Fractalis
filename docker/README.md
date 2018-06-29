### About
This folder contains all files necessary to setup the Fractalis service in a production environment.

### Usage
We assume that Docker and Docker-Compose are already installed on the system and 
are up-to-date. It is possible to check this by opening a terminal and running 
the following commands: 
```
> docker --version
docker version 18.03.0-ce, build 0520e24

> docker-compose --version
docker-compose version 1.20.1, build 5d8c71b
```
If these commands fail or if the versions are much older than the one displayed 
above please consult https://docs.docker.com/install/ and 
https://docs.docker.com/compose/install/

If docker is properly installed on your system please run the following commands:
```
> git clone https://git-r3lab.uni.lu/Fractalis/fractalis.git
> cd fractalis/docker
> docker-compose up
```
The last command might require root access to connect with the Docker engine. 
Depending on your network connection, this step will take a few minutes. Once 
all the services are up and running you can open Chrome, Firefox, or Safari and 
navigate to `http://localhost` or, if you use docker-machine, to http://`docker-machine ip`. 
If you see the 
Fractalis welcome screen, your system just became a Fractalis node that can be used 
for statistical computation, as long as Docker is running. If this fails for you, 
make sure docker is properly installed and port 80 and 443 are not used by other 
services on your system. If they are either stop the services or use the environment
variables `FRACTALIS_HTTP_PORT` and `FRACTALIS_HTTPS_PORT` to change the ports
used by Fractalis.

### Configuration (Fractalis / Celery / Flask)
1. Modify [docker/fractalis/config.py](config/fractalis/config.py) before `running docker-compose up`.

2. Replace the [dummy certificates](config/nginx/certs) with your own. The included one are only for development purposes.

Tip: Use the [default settings](../fractalis/config.py) as an example for your own configuration file.
Please note, that all this files combines [Flask settings](http://flask.pocoo.org/docs/0.12/config/), [Celery settings](http://docs.celeryproject.org/en/latest/userguide/configuration.html), and Fractalis settings, which are all listed and documented within this file. 
Please don't overwrite default settings if you don't know what you are doing. This might have severe implications for security or might cause Fractalis to not work correctly.


### Configuration (Nginx)
- (Mandatory) **Change the certificates!!** The certificates in `./config/nginx/certs` are only dummy certs for development. Do not use them in production! You can do this by replacing the dummy certs with your own or change the path in `docker-compose.yml`.
- (Optional) Modify `./config/nginx/conf.d/default.conf` to whatever you want. Please be aware that you are within a Docker network and special conventions apply.
- (Optional) You don't have to use (the included) nginx. Strip the service out of `docker-compose.yml` and just make your own http proxy listen to gunicorn at port 5000.
