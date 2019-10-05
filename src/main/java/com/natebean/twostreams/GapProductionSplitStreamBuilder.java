package com.natebean.twostreams;

import java.util.Properties;

import com.natebean.models.GapLog;
import com.natebean.models.GapLogProductionLogSplitRecord;
import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.producers.GapLogProducer;
import com.natebean.producers.ProductionLogProducer;
import com.natebean.utils.AppStreamBuilder;
import com.natebean.utils.StreamHelper;

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

import io.confluent.common.utils.TestUtils;

/*
 * Consumes gap log records
 * Has a global store of production log records
 * Split gap log records by production log records.
 * Producing one or more records
 * Confirming all duration of the gap log records is represented
 *  Creating dummy production log records if needed
 * Requirement: key is sidID:sysID:uniqueID for both sides
 * TODO Deal with gaps in production log data
 * TODO deal with overlapping production log data
 * TODO Convert to millisecond epoch 
 */
public final class GapProductionSplitStreamBuilder implements AppStreamBuilder {

    static final String STREAM_OUTPUT = "gap-production-log-split-json";
    public static final String STATE_STORE_NAME = "productionLogStore";
    private String broker;

    public static void main(final String[] args) {

        final String broker = StreamHelper.parseBroker(args);
        GapProductionSplitStreamBuilder sut = new GapProductionSplitStreamBuilder(broker);
        KafkaStreams streams = sut.buildStream();
        StreamHelper.startStreamBlocking(streams);

    }

    public GapProductionSplitStreamBuilder(String broker) {
        this.broker = broker;
    }

    @Override
    public KafkaStreams buildStream() {

        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfiguration(broker, true));

        streams.setGlobalStateRestoreListener(new MyStateRestoreListener());
        streams.setStateListener(new StateStreamListener());

        return streams;
    }

    public void createStream(StreamsBuilder builder) {

        builder.globalTable(ProductionLogProducer.SIMPLE_JSON_TOPIC,
                Materialized.<String, ProductionLog, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_NAME));

        KStream<String, GapLog> gapLogStream = builder.stream(GapLogProducer.SIMPLE_JSON_TOPIC,
                Consumed.with(Serdes.String(), new JSONSerde<GapLog>()));

        gapLogStream.flatTransformValues(GapProductionSplitTransformer::new).to(STREAM_OUTPUT,
                Produced.with(Serdes.String(), new JSONSerde<GapLogProductionLogSplitRecord>()));

    }

    public Properties getStreamsConfiguration(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "gap-production-log-split-stream-4-thread");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    public Properties getStreamsConfiguration(String bootstrapServers, boolean useTempLocation) {
        // Use a temporary directory for storing state, which will be automatically
        // removed after the test.
        Properties props = getStreamsConfiguration(bootstrapServers);
        if (useTempLocation) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        }
        return props;
    }

}
