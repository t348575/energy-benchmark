FROM mcr.microsoft.com/openjdk/jdk:21-ubuntu
RUN apt-get update && \
    apt-get install -y pssh nano libpq-dev python3 python3-pip
RUN pip3 install ydb psycopg2 numpy requests
COPY src/gen-key.sh /
COPY src/set-keys.sh /
COPY src/tpcc-config.xml /
COPY src/run.sh /
CMD ["tail", "-f", "/dev/null"]