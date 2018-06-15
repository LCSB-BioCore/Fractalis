FROM centos:7
RUN mkdir /app/ \
    && rm -f /etc/yum.repos.d/* \
    && printf "[lcsb-epel]\nname=LCSB epel Repo\nbaseurl=http://lcsb-cent-mirr-server.uni.lu/epel/7/x86_64/\nenabled=1" > /etc/yum.repos.d/lcsb-epel.repo \
    && printf "[lcsb-updates]\nname=LCSB Updates Repo\nbaseurl=http://lcsb-cent-mirr-server.uni.lu/CentOS/7/updates/x86_64/\nenabled=1" > /etc/yum.repos.d/lcsb-updates.repo \
    && printf "[lcsb-extras]\nname=LCSB Extras Repo\nbaseurl=http://lcsb-cent-mirr-server.uni.lu/CentOS/7/extras/x86_64/\nenabled=1" > /etc/yum.repos.d/lcsb-extras.repo \
    && printf "[lcsb-base]\nname=LCSB Base Repo\nbaseurl=http://lcsb-cent-mirr-server.uni.lu/CentOS/7/os/x86_64/\nenabled=1" > /etc/yum.repos.d/lcsb-base.repo \
    && rm -rf /var/cache/yum \
    && yum clean all \
    && yum install --nogpg -y python34 python34-pip python34-devel readline-devel libcurl-devel libxml2-devel R wget \
    && R -e 'source("https://bioconductor.org/biocLite.R"); biocLite(); biocLite("limma", "DESeq2")'
COPY tests/ /app/tests/
WORKDIR /app/
ARG SDIST
COPY $SDIST /app/
RUN pip3 install -i https://pypi.lcsb.uni.lu/simple fractalis-*.tar.gz gunicorn
