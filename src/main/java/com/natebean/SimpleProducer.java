package com.natebean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;


public class SimpleProducer{
    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    
        IntStream.range(101, 200)
                .mapToObj(val -> new ProducerRecord<>(SimpleConsumer.TOPIC_NAME, Integer.toString(val), Integer.toString(val)))
                .forEach(producer::send);

        producer.flush();

    }
}