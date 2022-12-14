FROM flink:1.16.0-scala_2.12-java8

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev && \
wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
tar -xvf Python-3.7.9.tgz && \
cd Python-3.7.9 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# install PyFlink
RUN pip3 install apache-flink==1.16.0 apache-flink-libraries==1.16.0
# COPY apache-flink*.tar.gz /
# RUN pip3 install /apache-flink-libraries*.tar.gz && pip3 install /apache-flink*.tar.gz

# Apache
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-pulsar/1.16.0/flink-connector-pulsar-1.16.0.jar -P /home/flink-jars/
# Didn't need this one
# RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.16.0/flink-avro-1.16.0.jar -P /home/flink-jars/
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro/1.16.0/flink-sql-avro-1.16.0.jar -P /home/flink-jars/
RUN wget https://repo1.maven.org/maven2/io/streamnative/connectors/flink-sql-connector-pulsar/1.15.1.1/flink-sql-connector-pulsar-1.15.1.1.jar -P /home/flink-jars/
# Stream native
# RUN wget https://search.maven.org/remotecontent?filepath=io/streamnative/connectors/flink-connector-pulsar/1.15.1.2/flink-connector-pulsar-1.15.1.2.jar -P /home/flink-jars/flink-connector-pulsar-1.15.1.2.jar
# RUN wget https://search.maven.org/remotecontent?filepath=io/streamnative/connectors/flink-sql-connector-pulsar/1.15.1.2/flink-sql-connector-pulsar-1.15.1.2.jar -P /home/flink-jars/flink-sql-connector-pulsar-1.15.1.2.jar
