package com.natebean.firstapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class LookupStringStream {

    static final String STREAM_OUTPUT = "streams-string-output";
    static final String STATE_STORE_NAME = "stringStore";

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lookup-string-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(SimpleStringProducer.SIMPLE_STRING_TOPIC,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_NAME));

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
            readState(streams);
            findKeyRange(streams);
            latch.await();
        } catch (final Throwable e) {
            System.out.println(e.getMessage().toString());
            System.exit(1);
        }
        System.exit(0);
    }

    public static void readState(KafkaStreams streams) throws InterruptedException {

        ReadOnlyKeyValueStore<String, String> keyValueStore = waitUntilStoreIsQueryable(STATE_STORE_NAME,
                QueryableStoreTypes.keyValueStore(), streams);

        System.out.println("First readState: " + keyValueStore.approximateNumEntries());
        // KeyValueIterator<Integer, Integer> range = keyValueStore.all();
        // while(range.hasNext()){
        // KeyValue<Integer, Integer> next = range.next();
        // System.out.println("sum for key: " + next.key + ": " + next.value);
        // }
        // range.close();

    }

    public static void findKeyRange(KafkaStreams streams) throws InterruptedException {
        ReadOnlyKeyValueStore<String, String> keyValueStore = waitUntilStoreIsQueryable(STATE_STORE_NAME,
                QueryableStoreTypes.keyValueStore(), streams);

        System.out.println("Ranged Search Starting " + new Timestamp(System.currentTimeMillis()).getTime());
        KeyValueIterator<String, String> range = keyValueStore.range("1:1", "1:2");
        System.out.println("Ranged Search Done " + new Timestamp(System.currentTimeMillis()).getTime());
        Integer recordCount = 0;
        Integer recordCountFilter = 0;
        while (range.hasNext()) {
            KeyValue<String, String> next = range.next();
            recordCount++;
            if(Integer.parseInt(next.value) > 750) recordCountFilter++;
            // System.out.println("key: " + next.key + ": " + next.value);
        }
        range.close();
        System.out.println(recordCount + " : " + recordCountFilter);

    }

    // Example: Wait until the store of type T is queryable. When it is, return a
    // reference to the store.
    public static <T> T waitUntilStoreIsQueryable(final String storeName,
            final QueryableStoreType<T> queryableStoreType, final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }

}
