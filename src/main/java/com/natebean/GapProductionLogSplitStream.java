package com.natebean;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.natebean.models.GapLog;
import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.producers.GapLogProducer;
import com.natebean.producers.ProductionLogProducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public final class GapProductionLogSplitStream {

    static final String STREAM_OUTPUT = "gap-production-log-split-json";
    public static final String STATE_STORE_NAME = "productionLogStore";

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "gap-production-log-split-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(ProductionLogProducer.SIMPLE_JSON_TOPIC,
                Materialized.<String, ProductionLog, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_NAME));

        KStream<String, GapLog> gapLogStream = builder.stream(GapLogProducer.SIMPLE_JSON_TOPIC,
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        gapLogStream.flatTransformValues(() -> new GapProductionLogSplitTransformer(), STATE_STORE_NAME)
                .to(STREAM_OUTPUT, Produced.with(Serdes.String(), new JSONSerde<GapLogProductionLogSplitRecord>()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("closing stream");
                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("Starting");
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
