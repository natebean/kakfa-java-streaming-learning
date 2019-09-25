package com.natebean;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class SimpleJSONProducer {

    
    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-typed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        // final KStream<String, PageView> views = builder.stream("streams-pageview-input", Consumed.with(Serdes.String(), new JSONSerde<>()));

        // final KTable<String, UserProfile> users = builder.table("streams-userprofile-input", Consumed.with(Serdes.String(), new JSONSerde<>()));

        // final KStream<WindowedPageViewByRegion, RegionCount> regionCount = views
        //     .leftJoin(users, (view, profile) -> {
        //         final PageViewByRegion viewByRegion = new PageViewByRegion();
        //         viewByRegion.user = view.user;
        //         viewByRegion.page = view.page;

        //         if (profile != null) {
        //             viewByRegion.region = profile.region;
        //         } else {
        //             viewByRegion.region = "UNKNOWN";
        //         }
        //         return viewByRegion;
        //     })
        //     .map((user, viewRegion) -> new KeyValue<>(viewRegion.region, viewRegion))
        //     .groupByKey(Grouped.with(Serdes.String(), new JSONSerde<>()))
        //     .windowedBy(TimeWindows.of(Duration.ofDays(7)).advanceBy(Duration.ofSeconds(1)))
        //     .count()
        //     .toStream()
        //     .map((key, value) -> {
        //         final WindowedPageViewByRegion wViewByRegion = new WindowedPageViewByRegion();
        //         wViewByRegion.windowStart = key.window().start();
        //         wViewByRegion.region = key.key();

        //         final RegionCount rCount = new RegionCount();
        //         rCount.region = key.key();
        //         rCount.count = value;

        //         return new KeyValue<>(wViewByRegion, rCount);
        //     });

        // write to the result topic
        // regionCount.to("streams-pageviewstats-typed-output");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}