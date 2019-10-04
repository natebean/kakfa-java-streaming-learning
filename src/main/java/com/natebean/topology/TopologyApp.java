package com.natebean.topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.natebean.models.GapLog;
import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.producers.GapLogProducer;
import com.natebean.producers.ProductionLogProducer;
import com.natebean.topology.processors.PrintProcessor;
import com.natebean.topology.processors.ProductionLogProcessor;
import com.natebean.utils.StreamHelper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import io.confluent.common.utils.TestUtils;

public class TopologyApp {

    public static void main(String[] args) {

        Topology builder = new Topology();
        final String broker = StreamHelper.parseBroker(args);

        // Creating producer for emit records when global state changes, since you can't connect
        // to a global sub-topology
        KafkaProducer<String, ProductionLog> productionLogChangedProducer = getProductionLogProducer();

        createStream(builder, productionLogChangedProducer); // reference parms for objects

        final KafkaStreams streams = new KafkaStreams(builder, getStreamsConfiguration(broker));

        StreamHelper.cleanUpStream(streams);

        //Show topology
        System.out.println(builder.describe());

        //Used to share state between threads, didn't need it 
        StateShare sharedState = new StateShare();

        // Watching for global state is ready for use
        // Just a test, not needed in this case
        streams.setGlobalStateRestoreListener(new MyStateRestoreListener(streams, sharedState));

        // Watching state of stream
        // Just a test, not needed in this case
        streams.setStateListener(new StateStreamListener());

        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-threads") {
            @Override
            public void run() {
                System.out.println("Closing Stream");
                productionLogChangedProducer.close();
                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("Starting");
            streams.start(); // blocking until running state
            System.out.println("Pass Started");
            //Will not get here until global table is backfilled/restored
            ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalState = sharedState.getGlobalState();
            System.out.println("Main thread: " + globalState.approximateNumEntries());
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }


    public static void createStream(Topology builder, KafkaProducer<String, ProductionLog> productionLogProducer) {

        // withLoggingDisabled required for global table
        StoreBuilder<KeyValueStore<String, ProductionLog>> plStoreSupplier = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore("plStore"), Serdes.String(),
                        new JSONSerde<ProductionLog>())
                .withLoggingDisabled();

        // When stream is started the global state is "backfilled"/restored, after than you are responsible for data updates via the "globalProcessor"
        // Global state is in it's own Sub-topology and it doesn't appear it can be connected to anything else.
        builder.addGlobalStore(plStoreSupplier, "plStore", Serdes.String().deserializer(),
                new JSONSerde<ProductionLog>(), ProductionLogProducer.SIMPLE_JSON_TOPIC, "globalProcessor",
                () -> new ProductionLogProcessor("state", productionLogProducer));

        builder.addSource("gapLogSource", Serdes.String().deserializer(), new JSONSerde<GapLog>(),
                GapLogProducer.SIMPLE_JSON_TOPIC)

                .addProcessor("Process03", () -> new PrintProcessor<GapLog>("processor03", "plStore"), "gapLogSource");

    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-app-global-test-04");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        return props;
    }

    private static KafkaProducer<String, ProductionLog> getProductionLogProducer() {
        final String bootstrapServers = "localhost:9092";
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerde.class);

        final KafkaProducer<String, ProductionLog> producer = new KafkaProducer<>(props);

        return producer;
    }

}