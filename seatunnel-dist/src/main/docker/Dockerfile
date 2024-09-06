FROM openjdk:8

ARG VERSION

COPY ./target/apache-seatunnel-${VERSION}-bin.tar.gz /opt/

RUN cd /opt && \
    tar -zxvf apache-seatunnel-${VERSION}-bin.tar.gz && \
    mv apache-seatunnel-${VERSION} seatunnel && \
    rm apache-seatunnel-${VERSION}-bin.tar.gz

WORKDIR /opt/seatunnel
