package com.natebean.twostreams;

import java.util.Properties;

import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.producers.ProductionLogProducer;
import com.natebean.utils.AppStreamBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import io.confluent.common.utils.TestUtils;

/*
 * Will take changes in ProductionLog topic
 * when it changes, it will check that the GapProductionSplitStream global state has that record updated
 * then will produces messages on the gap-log-json topic of effected gap-log records that need to be reprocessed
 *
 */
public final class ProductionLogChangedStreamBuilder implements AppStreamBuilder {

    static final String STREAM_OUTPUT = "gap-log-production-log-changed-json";
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> productionLogGlobalState;
    private String broker;


    public ProductionLogChangedStreamBuilder(String broker,ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> productionLogGlobalState){
        this.broker = broker;
        this.productionLogGlobalState = productionLogGlobalState;
    } 

    public KafkaStreams buildStream() {
        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), getStreamsConfiguration(broker, true));
        return streams;
    }

    public void createStream(StreamsBuilder builder) {

        builder.stream(ProductionLogProducer.SIMPLE_JSON_TOPIC,
                Consumed.with(Serdes.String(), new JSONSerde<ProductionLog>()))

                .process(() -> new ProductionLogChangeProcessor(productionLogGlobalState));

    }

    public Properties getStreamsConfiguration(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "gap-log-production-log-changed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

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
