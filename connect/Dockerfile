FROM confluentinc/cp-kafka-connect:8.3.0

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:8.3.0

ARG DEBEZIUM_VERSION=3.7.0.Final
RUN python3 -c "import os, tarfile, urllib.request; v = os.environ['DEBEZIUM_VERSION']; \
tarfile.open(fileobj=urllib.request.urlopen(f'https://repo.maven.apache.org/maven2/io/debezium/debezium-connector-postgres/{v}/debezium-connector-postgres-{v}-plugin.tar.gz'), mode='r|gz').extractall('/usr/share/confluent-hub-components')"
