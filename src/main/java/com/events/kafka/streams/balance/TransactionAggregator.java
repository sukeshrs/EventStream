package com.events.kafka.streams.balance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TransactionAggregator {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionAggregator.class);
    public static void main(String[] args) {
        Properties config = getStreamConfig();
        LOGGER.info("Stream config {}" ,config);
     //   System.out.println(config);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String , String > kStream = builder.stream("bank-transactions");
        kStream.to("bank-balance-aggregator");
        final CountDownLatch latch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(builder.build() , config);
        streams.start();
        LOGGER.info("streams : {}" , streams.toString());
        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getStreamConfig() {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG , "transaction-aggregator");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");

        //disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        //For exactly once processing
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG , StreamsConfig.EXACTLY_ONCE);
        return config;
    }
}
