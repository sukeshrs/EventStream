package com.events.kafka.streams.balance;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionsProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionsProducer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        //Kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);

        int i = 0;

        while (true) {
            LOGGER.info("Producing batch" + i);
                sendTheMessage(producer, "roger");
                sendTheMessage(producer, "fish");
                sendTheMessage(producer, "cilic");
                i++;
        }
    }

    private static void sendTheMessage(Producer<String, String> producer, String roger) {
        try {
            Future<RecordMetadata> produceFuture = producer.send(generateRandomTransaction(roger));
            RecordMetadata metaData  = produceFuture.get();
            Thread.sleep(100);
            LOGGER.info("Record meta data {} , {} , {}" , metaData.offset() , metaData.partition(),metaData.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            LOGGER.error("Issue in sending the message" , e);
        }
    }

    public static ProducerRecord<String, String> generateRandomTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);

        Instant now = Instant.now();
        // we write the data to the json document
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>("bank-transactions", name, transaction.toString());

    }
}
