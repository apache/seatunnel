FROM openjdk:8

RUN apt update ; \
    apt install -y wget ;

ARG VERSION=2.3.3

RUN wget "https://archive.apache.org/dist/seatunnel/$VERSION/apache-seatunnel-$VERSION-bin.tar.gz" -P /opt && \
    cd /opt && \
    tar -zxvf apache-seatunnel-$VERSION-bin.tar.gz && \
    mv apache-seatunnel-$VERSION seatunnel && \
    rm apache-seatunnel-$VERSION-bin.tar.gz && \
    wget "https://repo.maven.apache.org/maven2/org/apache/seatunnel/seatunnel-hadoop3-3.1.4-uber/$VERSION/seatunnel-hadoop3-3.1.4-uber-$VERSION-optional.jar" -P /opt/seatunnel/lib

WORKDIR /opt/seatunnel
