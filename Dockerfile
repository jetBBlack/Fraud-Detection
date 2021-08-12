FROM java:8

WORKDIR /opt/fraudetection

FROM python:3.8 AS builder
COPY ./ /opt/frauddetection
WORKDIR /opt/frauddetection
RUN pip install apache-flink
RUN pip install kafka-python

FROM flink:1.11.0-scala_2.11

WORKDIR /opt/flink/bin

COPY --from=builder /opt/frauddetection/target/frauddetection-*.jar /opt/frauddetection.jar

