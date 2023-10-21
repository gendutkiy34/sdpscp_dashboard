FROM python:3.11.6-slim-bullseye

RUN apt-get update
RUN apt-get -y install unzip
RUN apt-get install libaio1
RUN apt-get -y install gcc
RUN apt-get update && apt-get install -y iputils-ping
RUN mkdir /opt/oracle
RUN mkdir /opt/app
COPY ./requirements.txt /opt
COPY ./instantclient-basic-linux.x64-12.2.0.1.0.zip /opt/oracle
RUN unzip /opt/oracle/instantclient-basic-linux.x64-12.2.0.1.0.zip -d /opt/oracle
RUN pip install -r /opt/requirements.txt


