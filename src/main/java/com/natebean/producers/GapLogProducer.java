package com.natebean.producers;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Properties;
import java.util.stream.IntStream;

import com.natebean.models.GapLog;
import com.natebean.models.JSONSerde;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class GapLogProducer {

    static public final String SIMPLE_JSON_TOPIC = "gap-log-json";

    public static void main(final String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerde.class);

        final KafkaProducer<String, GapLog> producer = new KafkaProducer<>(props);

        final long startinglastEndTime = LocalDate.of(2000, 1, 1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        long lastEndTime = startinglastEndTime;

        // Random rand = new Random();

        for (Integer sidId : IntStream.range(1, 2).toArray()) {
            for (Integer sysId : IntStream.range(1, 4).toArray()) {
                for (Integer gapLogId : IntStream.range(1, 100000).toArray()) {
                    String keyString = sidId + ":" + sysId + ":" + gapLogId;

                    if (gapLogId == 1)
                        lastEndTime = startinglastEndTime;

                    GapLog gl = GapLogFactory.getNextGapLogRecord(sidId, sysId, gapLogId, lastEndTime);
                    ProducerRecord<String, GapLog> record = new ProducerRecord<>(SIMPLE_JSON_TOPIC, keyString, gl);
                    lastEndTime = gl.endTime;
                    producer.send(record);
                }
            }
        }

        producer.flush();
        producer.close();

    }
}