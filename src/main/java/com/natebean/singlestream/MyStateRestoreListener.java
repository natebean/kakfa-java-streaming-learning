package com.natebean.singlestream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class MyStateRestoreListener implements StateRestoreListener {



    private KafkaStreams streams;
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalState;
    private StateShare sharedState;

    public MyStateRestoreListener(KafkaStreams streams,
            StateShare ss) {
        this.streams = streams;
        this.sharedState = ss;
    }


	@Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset,
            long endingOffset) {
        System.out.println("starting: " + storeName + ":" + startingOffset + ":" + endingOffset);

    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset,
            long numRestored) {
        // System.out.println("Restored: " + storeName + ":" + numRestored);
        System.out.print(".");

    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        System.out.println("Done");
        System.out.println("Restored Ended: " + storeName + ":" + totalRestored);
        globalState = streams.store("plStore", QueryableStoreTypes.keyValueStore());
        System.out.println("Global State after Restore " + storeName + ":" + globalState.approximateNumEntries());
        sharedState.setGlobalState(globalState);

    }

   
}