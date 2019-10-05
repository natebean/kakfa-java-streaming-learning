package com.natebean.twostreams;

import java.util.concurrent.CountDownLatch;

import com.natebean.utils.StreamHelper;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class GapProductionSplitStreamWithReruns {

    public static void main(String[] args) {
        final String broker = StreamHelper.parseBroker(args);

        final KafkaStreams mainStream = createMainStream(broker);

        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c

        try {
            System.out.println("Main Starting");
            mainStream.start();

            // final KafkaStreams changeLogStream = createChangeStream(broker, mainStream
            //         .store(GapProductionSplitStreamBuilder.STATE_STORE_NAME, QueryableStoreTypes.keyValueStore()));

            Runtime.getRuntime().addShutdownHook(new Thread("streams-threads") {
                @Override
                public void run() {
                    System.out.println("changeStream Stream Closing");
                    // changeLogStream.close();
                    System.out.println("Main Stream Closing");
                    mainStream.close();
                    latch.countDown();
                }
            });

            System.out.println("Main Running");
            System.out.println("changeStream Starting");
            // changeLogStream.start();
            System.out.println("changeStream Running");
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);

    }

    public static KafkaStreams createMainStream(String broker) {

        GapProductionSplitStreamBuilder builder = new GapProductionSplitStreamBuilder(broker);
        return builder.buildStream();
    }

    public static KafkaStreams createChangeStream(String broker,
            ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> productionLogGlobalState) {

        ProductionLogChangedStreamBuilder builder = new ProductionLogChangedStreamBuilder(broker,
                productionLogGlobalState);
        return builder.buildStream();

    }

}