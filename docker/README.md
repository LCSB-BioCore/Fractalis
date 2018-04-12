### About
This folder contains all files necessary to setup the Fractalis service in a production environment.

### Usage
`docker-compose up` That's all! This will expose the service on port 443 and 80 by default.
This behavior can be changed by setting the environment variables `FRACTALIS_HTTP_PORT` and `FRACTALIS_HTTPS_PORT`.
For more detailed information please look into the files. They are rather self-explanatory and good place to do own modifications.

### Configuration (Fractalis / Celery / Flask)
1. Modify [docker/fractalis/config.py](https://git-r3lab.uni.lu/Fractalis/fractalis/blob/master/docker/config/fractalis/config.py) before `running docker-compose up`.

2. Replace the [dummy certificates](https://git-r3lab.uni.lu/Fractalis/fractalis/tree/master/docker/config/nginx/certs) with your own. The included one are only for development purposes.

Tip: Use the [default settings](https://git-r3lab.uni.lu/Fractalis/fractalis/blob/master/fractalis/config.py) as an example for your own configuration file.
Please note, that all this files combines [Flask settings](http://flask.pocoo.org/docs/0.12/config/), [Celery settings](http://docs.celeryproject.org/en/latest/userguide/configuration.html), and Fractalis settings, which are all listed and documented within this file. 
Please don't overwrite default settings if you don't know what you are doing. This might have severe implications for security or might cause Fractalis to not work correctly.


### Configuration (Nginx)
- (Mandatory) **Change the certificates!!** The certificates in `./config/nginx/certs` are only dummy certs for development. Do not use them in production! You can do this by replacing the dummy certs with your own or change the path in `docker-compose.yml`.
- (Optional) Modify `./config/nginx/conf.d/default.conf` to whatever you want. Please be aware that you are within a Docker network and special conventions apply.
- (Optional) You don't have to use (the included) nginx. Strip the service out of `docker-compose.yml` and just make your own http proxy listen to gunicorn at port 5000.
