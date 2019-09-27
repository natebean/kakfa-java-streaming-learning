package com.natebean.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;

public class ProductionLogProducer {

    static public final String SIMPLE_JSON_TOPIC = "production-log-json";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerde.class);

        final KafkaProducer<String, ProductionLog> producer = new KafkaProducer<>(props);

        Random rand = new Random();

        for (Integer sidId : IntStream.range(1, 44).toArray()) {
            for (Integer sysId : IntStream.range(1, rand.nextInt(35)).toArray()) {
                for (Integer productionId : IntStream.range(1, 1700).toArray()) {
                    String keyString = sidId + ":" + sysId + ":" + productionId;

                    ProducerRecord<String, ProductionLog> record = new ProducerRecord<>(SIMPLE_JSON_TOPIC, keyString,
                            new ProductionLog(sidId, sysId, productionId));
                    producer.send(record);
                }

            }

        }

        producer.flush();
        producer.close();

    }
}