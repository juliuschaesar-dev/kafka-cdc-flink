FROM confluentinc/cp-kafka-connect:8.3.0

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:8.3.0 && \
    confluent-hub install --no-prompt debezium/debezium-connector-postgresql:3.1.2
