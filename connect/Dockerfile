FROM confluentinc/cp-kafka-connect-base:7.6.12

# Avro converter + Debezium Postgres plugin. Pinned to 2.5.4-2, not :latest --
# newer builds require Java 17 and this base image runs on Java 11.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.6.1 && \
    confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.5.4-2
