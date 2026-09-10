FROM flink:1.18-scala_2.12-java11

# Install Python 3 + pip (Flink image is Debian-based)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink-job

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Kafka SQL connector jar (matches Flink 1.18)
RUN wget -O /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

COPY job.py .

CMD ["python", "job.py"]