package com.events.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class StreamStarterApp {
    static final Logger logger = LoggerFactory.getLogger(StreamStarterApp.class);
    public static void main(String[] args) {
        //Set the properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "EventWordCount");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //Stream from Kafka
        KStream<String, String> kafkaStream = builder.stream("word-count-source");
        //Map values to lowercase
        //Flat map values to split by space
        //Select key to apply a key
        //Group by key before aggregation
        //Count occurrences
        KTable<String, Long> wordCounts =  kafkaStream.mapValues(text -> text.toLowerCase())
                .flatMapValues(lowerCaseText -> Arrays.asList(lowerCaseText.split(" ")))
                .selectKey(((key, word) -> word ))
                .groupByKey()
                .count();
        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        wordCounts.toStream().to("word-count-target", Produced.with(stringSerde, longSerde));
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        logger.info("streams", streams);
        System.out.println("streams : " + streams.toString());
        //Write results back to kafka

        //Clean up
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
