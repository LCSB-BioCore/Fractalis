[![Build Status](https://git-r3lab.uni.lu/sascha.herzinger/fractalis/badges/master/build.svg)](https://git-r3lab.uni.lu/sascha.herzinger/fractalis/builds/)
[![Coverage Report](https://git-r3lab.uni.lu/sascha.herzinger/fractalis/badges/master/coverage.svg)](https://git-r3lab.uni.lu/sascha.herzinger/fractalis/builds/)

### About
This is the back-end component of the Fractalis project. It is a computational node that is responsible for the MicroETL process and the execution of analytical tasks. See https://fractalis.lcsb.uni.lu/

### Demo
Please have a look at this playlist to see a demo of the visual aspects of Fractalis: [Playlist](https://www.youtube.com/playlist?list=PLNvp9GB9uBmH1NNAf-qTyj_jN2aCPISFU).

### Installation (Docker)
The easiest and most convenient way to deploy Fractalis is using Docker.
All necessary information can be found [here](docker).

### Installation (Manual)
If you do not want to use docker or want a higher level of control of the several components, that's fine. In fact it isn't difficult to setup Fractalis manually:

- Install and run [Redis](https://redis.io/), which is available on most Linux distributions. This instance must be accessible by the web service and the workers.
- Install and run [RabbitMQ](https://www.rabbitmq.com/), which is available on most Linux distributions. This instance must be accessible by the web service and the workers.
- Install Fractalis via `pip3 install fractalis`. Please note that Fractalis requires Python3.4 or higher. This must be installed on all machines that will run the web service or the workers.
- Install required all required R packages. We won't list these packages excplicitely, as they can change frequently. Please refer instead to the [Dockerfile](https://git-r3lab.uni.lu/Fractalis/fractalis/blob/master/docker/Dockerfile), which is *always* up-to-date, as a new version of Fractalis is only released when the Docker image passes all tests. This must be installed on all machines that will run the web service or the workers.
- Run and expose the Fractalis web service with whatever tools you want. We recommend **gunicorn** and **nginx**, but others should work, too.
- Run the celery workers on any machine that you want within the same network. (For a simple setup this can be the very same machine that the web service runs on).

Note: The [docker-compose.yml](docker/docker-compose.yml) describes how the different services are started and how they connect with each other.

### Configuration
Use the environment variable `FRACTALIS_CONFIG` to define the configuration file path.
This variable must be a) a valid python file (.py) and b) be available on all instances that host a Fractalis web service or a Fractalis worker.

Tip: Use the [default settings](fractalis/config.py) as an example for your own configuration file.
Please note, that all this files combines [Flask settings](http://flask.pocoo.org/docs/0.12/config/), [Celery settings](http://docs.celeryproject.org/en/latest/userguide/configuration.html), and Fractalis settings, which are all listed and documented within this file. 
Please don't overwrite default settings if you don't know what you are doing. This might have severe implications for security or might cause Fractalis to not work correctly.

### Add new analytics
This paragraph only describes how to add the statistical analysis part in R or Python. For the visualisation please refer to the [Fractal.js](https://github.com/LCSB-BioCore/Fractal.js/blob/master/README.md) repository.
Adding new scripts to Fractalis is a matter of inheriting from the [AnalyticTask](https://github.com/LCSB-BioCore/Fractalis/blob/master/fractalis/analytics/task.py) class and well... knowing how to write Python and R scripts.
There are a few things that are enforced by the parent class, such as a task name and the existence of a main method, which is communicated to the developer by readable error messages.
Implementing those will result in a distributable task without any required knowledge of the surrounding frameworks.
For an example please have a look at the [correlation analysis script](https://github.com/LCSB-BioCore/Fractalis/blob/master/fractalis/analytics/tasks/correlation/main.py).

Input to the main method are either the parameters submitted by the client or, in the case of special data ids ("$123654789$"), the data frame associated with he data id.

Output of the main method must be JSON serializable. The content however is up to the developer.  

### Add support for new services
Please refer to [this document](fractalis/data).

### Citation
Manuscript is in preparation.
