package com.natebean;

import java.util.Properties;

import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.processors.ProductionLogProcessor;
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

        StoreBuilder<KeyValueStore<String, ProductionLog>> productionLogStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("productionLogStore"), Serdes.String(), new JSONSerde<ProductionLog>());

        builder.addSource("ProductionLogSource", Serdes.String().deserializer(), new JSONSerde<ProductionLog>(),
                ProductionLogProducer.SIMPLE_JSON_TOPIC)
        
                // .addGlobalStore(productionLogStoreSupplier, "ProductionLogSource", Serdes.String(), new JSONSerde<ProductionLog>(), opic, processorName, stateUpdateSupplier)

                .addProcessor("Process", () -> new ProductionLogProcessor(), "ProductionLogSource");

        // add the count store associated with the WordCountProcessor processor
        // .addStateStore(countStoreBuilder, "Process")

        // add the sink processor node that takes Kafka topic "sink-topic" as output
        // and the WordCountProcessor node as its upstream processor
        // .addSink("Sink", "sink-topic", "Process");
        final String broker = StreamHelpers.parseBroker(args);
        final KafkaStreams streams = new KafkaStreams(builder, getStreamsConfiguration(broker));
        streams.cleanUp(); //delete local state
        StreamHelpers.startStream(streams);

    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-app-01");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

}