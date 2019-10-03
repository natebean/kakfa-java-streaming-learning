package com.natebean;

import java.util.Properties;

import com.natebean.models.GapLog;
import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.processors.PrintProcessor;
import com.natebean.processors.ProductionLogProcessor;
import com.natebean.producers.GapLogProducer;
import com.natebean.producers.ProductionLogProducer;
import com.natebean.utils.StreamHelpers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class TopologyApp {

    public static void main(String[] args) {

        Topology builder = new Topology();

        StoreBuilder<KeyValueStore<String, ProductionLog>> plStoreSupplier = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore("plStore"), Serdes.String(),
                        new JSONSerde<ProductionLog>())
                .withLoggingDisabled();

        // builder.addSource("ProductionLogSource", Serdes.String().deserializer(), new
        // JSONSerde<ProductionLog>(),
        // ProductionLogProducer.SIMPLE_JSON_TOPIC)

        // builder.addGlobalStore(productionLogStoreSupplier,
        // "ProductionLogStateSource", Serdes.String().deserializer(),
        // new JSONSerde<ProductionLog>(), ProductionLogProducer.SIMPLE_JSON_TOPIC,
        // "globalProcessor",
        // () -> new ProductionLogProcessor("state"))

        // .addProcessor("Process", () -> new ProductionLogProcessor("processor"),
        // "ProductionLogSource");

        builder.addGlobalStore(plStoreSupplier, "plStore",
        Serdes.String().deserializer(),
        new JSONSerde<ProductionLog>(), ProductionLogProducer.SIMPLE_JSON_TOPIC,
        "globalProcessor",
        () -> new ProductionLogProcessor("state"));

        // .addProcessor("Process02", () -> new ProductionLogProcessor("processor02"),
        // "globalProcessor");
        builder.addSink("sink", "sink-topic", "globalProcessor");

        builder.addSource("gapLogSource", Serdes.String().deserializer(), new JSONSerde<GapLog>(),
                GapLogProducer.SIMPLE_JSON_TOPIC)

                .addProcessor("Process03", () -> new PrintProcessor<GapLog>("processor03", "plStore"), "gapLogSource");

        // add the count store associated with the WordCountProcessor processor
        // .addStateStore(countStoreBuilder, "Process")

        // add the sink processor node that takes Kafka topic "sink-topic" as output
        // and the WordCountProcessor node as its upstream processor
        // .addSink("Sink", "sink-topic", "Process");
        final String broker = StreamHelpers.parseBroker(args);
        final KafkaStreams streams = new KafkaStreams(builder, getStreamsConfiguration(broker));
        // streams.cleanUp(); // delete local state
        System.out.println(builder.describe());
        StreamHelpers.startStream(streams);

    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-app-global-test-02");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

}