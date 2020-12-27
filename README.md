# EventStream
  Read sentences  from a source topic and writes the target topic to an output topic
  Assuming zookeeper is running on the port 2181 and broker is on 9093
# Create the topics
  # create source topic
    kafka-topics.bat --create --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --topic word-count-source
  # create the target topic
    kafka-topics.bat --create --zookeeper localhost:2181 --partitions 2 --replication-factor 1 --topic word-count-target

# Consume the output topic
    kafka-console-consumer.bat --bootstrap-server localhost:9093 ^
    --topic word-count-target ^
    --from-beginning ^
    --formatter kafka.tools.DefaultMessageFormatter ^
    --property print.key=true ^
    --property print.value=true ^
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer