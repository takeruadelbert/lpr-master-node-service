FROM ubuntu
RUN apt-get update && apt-get install -y --no-install-recommends software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && apt-get install -y --no-install-recommends python3.8-minimal && \
    apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/pip3 /usr/bin/pip && ln -s /usr/bin/python3.8 /usr/bin/python && pip3 install virtualenv
ENV HOME=/home/lpr-master-node
COPY broker ${HOME}/broker/
COPY database ${HOME}/database/
COPY misc ${HOME}/misc/
COPY requirement ${HOME}/requirement/
COPY ["environment.env", "install-package.sh","main.py", "run.sh", "service.py", "${HOME}/"]
WORKDIR ${HOME}
RUN chmod +x install-package.sh && chmod +x run.sh && ./install-package.sh