package com.natebean.utils;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;

public class StreamHelper {
    /*
     * Note:  Blocking due to lacth.await()
     */
    public static void startStreamBlocking(KafkaStreams streams) {

        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-threads") {
            @Override
            public void run() {
                System.out.println("Closing Stream");
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

    public static void cleanUpStream(KafkaStreams streams) {
        try {
            streams.cleanUp();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when
            // executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println(
                    "Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    public static String parseBroker(String[] args) {
        String broker = "localhost:9092";

        if (args.length > 0) {
            broker = args[0];
            System.out.println("Broker: " + args[0]);
        } else {
            System.out.println("Default Broker: " + broker);
        }

        return broker;
    }
}