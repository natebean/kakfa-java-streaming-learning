package com.natebean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import java.util.stream.IntStream;
import java.util.Random;

public class SimpleProducer {
    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(props);

        Random rand = new Random();

        IntStream.range(1, 101).mapToObj(val -> new ProducerRecord<>(SimpleConsumer.TOPIC_NAME, val,
                rand.nextInt(1000))).forEach(producer::send);

        producer.flush();

    }
}