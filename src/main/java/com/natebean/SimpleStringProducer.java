package com.natebean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;
import java.util.Random;

public class SimpleStringProducer {

    public static String SIMPLE_STRING_TOPIC = "simple-string-topic";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random rand = new Random();

        // IntStream.range(1, 10000).mapToObj(val -> new ProducerRecord<>(SIMPLE_STRING_TOPIC, val, rand.nextInt(1000)))
        //         .forEach(producer::send);

        for (Integer sidId : IntStream.range(1, 44).toArray()) {
            for (Integer sysId : IntStream.range(1, rand.nextInt(35)).toArray()) {
                for (Integer productionId:  IntStream.range(1,1700).toArray()){
                    String keyString = sidId + ":" + sysId + ":" + productionId;

                    ProducerRecord<String, String> record = new ProducerRecord<>(SIMPLE_STRING_TOPIC,keyString, Integer.toString(rand.nextInt(1000)) );
                    producer.send(record);
                }

            }

        }

        producer.flush();

    }
}