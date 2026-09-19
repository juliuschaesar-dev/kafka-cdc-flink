FROM flink:1.20.5-scala_2.12-java17

# Kafka connector + Avro/Schema Registry format, plus JDBC + Postgres driver
# for the dimension lookup joins.
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.3/flink-sql-avro-confluent-registry-1.20.3.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.4.0-1.20/flink-connector-jdbc-3.4.0-1.20.jar \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar
