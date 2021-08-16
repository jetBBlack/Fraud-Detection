FROM java:8

WORKDIR /opt/fraudetection

FROM python:3.8 AS builder
RUN apt-get update \
    && apt-get install -yyq netcat

COPY ./ /opt/frauddetection
WORKDIR /opt/frauddetection
COPY requirements.txt requirements.txt
RUN pip intasll -r requirements.txt

FROM flink:1.11-java8

WORKDIR /opt/flink/bin



