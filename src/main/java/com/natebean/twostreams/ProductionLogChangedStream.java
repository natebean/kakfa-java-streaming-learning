package com.natebean.twostreams;

import java.util.Properties;

import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.producers.ProductionLogProducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import io.confluent.common.utils.TestUtils;

public final class ProductionLogChangedStream{

    static final String STREAM_OUTPUT = "gap-log-production-log-changed-json";

    public static void createStream(StreamsBuilder builder) {

        KStream<String, ProductionLog> stream = builder.stream(ProductionLogProducer.SIMPLE_JSON_TOPIC,
                Consumed.with(Serdes.String(), new JSONSerde<ProductionLog>()));

        // stream.process();

    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "gap-log-production-log-changed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    public static Properties getStreamsConfiguration(String bootstrapServers, boolean useTempLocation) {
        // Use a temporary directory for storing state, which will be automatically
        // removed after the test.
        Properties props = getStreamsConfiguration(bootstrapServers);
        if (useTempLocation) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        }
        return props;
    }
}
