package com.natebean.firstapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class SimpleConsumer {
    static final public String TOPIC_NAME = "first-java-topic";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,IntegerDeserializer.class );
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(properties);
        // consumer.subscribe(Collections.singleton(SimpleConsumer.TOPIC_NAME));
        consumer.subscribe(Collections.singleton(FirstStream.STREAM_OUTPUT));
        while (true) {
            final ConsumerRecords<Integer, Integer> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

            for (final ConsumerRecord<Integer, Integer> record : records) {
                System.out.println("key: " + record.key() + " : " + record.value());
            }
        }

    }
}