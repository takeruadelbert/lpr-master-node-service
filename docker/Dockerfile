FROM ubuntu
RUN apt-get update
RUN apt-get install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install python3.8 -y \
    python3-pip
RUN ln -s /usr/bin/pip3 /usr/bin/pip
RUN ln -s /usr/bin/python3.8 /usr/bin/python
RUN pip3 install virtualenv
ENV HOME=/home/lpr-master-node
COPY broker ${HOME}/broker/
COPY database ${HOME}/database/
COPY misc ${HOME}/misc/
COPY requirement ${HOME}/requirement/
COPY environment.env ${HOME}
COPY install-package.sh ${HOME}
COPY main.py ${HOME}
COPY run.sh ${HOME}
COPY service.py ${HOME}
WORKDIR ${HOME}
RUN chmod +x install-package.sh && chmod +x run.sh
RUN ./install-package.sh