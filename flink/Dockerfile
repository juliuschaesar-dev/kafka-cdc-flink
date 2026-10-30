FROM flink:2.2.1-scala_2.12-java17

# Kafka connector + Avro/Schema Registry format, plus JDBC + Postgres driver
# for the dimension lookup joins.
RUN wget -q -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.1.0-2.2/flink-connector-jdbc-core-4.1.0-2.2.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.1.0-2.2/flink-connector-jdbc-postgres-4.1.0-2.2.jar \
    https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar
