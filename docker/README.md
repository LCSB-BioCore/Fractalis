### About
This folder contains all files necessary to setup the Fractalis service in a production environment.

### Usage
`docker-compose up` That's all! This will expose the service on port 443 and 80. (Please read the entire document before doing this in a public production setup.)
For more detailed information please look into the files. They are rather self-explanatory and good place to do own modifications.

### Configuration (Fractalis / Celery / Flask)
You can configure nearly every aspect of Fractalis by setting the environment variable `FRACTALIS_CONFIG`.
Please have a look at [the Fractalis repository](https://git-r3lab.uni.lu/Fractalis/fractalis) for more details.

### Configuration (Nginx)
- (Mandatory) **Change the certificates!!** The certificates in `./config/nginx/certs` are only dummy certs for development. Do not use them in production! You can do this by replacing the dummy certs with your own or change the path in `docker-compose.yml`.
- (Optional) Modify `./config/nginx/conf.d/default.conf` to whatever you want. Please be aware that you are within a Docker network and special conventions apply.
- (Optional) You don't have to use (the included) nginx. Strip the service out of `docker-compose.yml` and just make your own http proxy listen to gunicorn at port 5000.
