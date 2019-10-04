package com.natebean.twostreams;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

public class MyStateRestoreListener implements StateRestoreListener {




    public MyStateRestoreListener() {
    }


	@Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset,
            long endingOffset) {
        System.out.println("starting: " + storeName + ":" + startingOffset + ":" + endingOffset);

    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset,
            long numRestored) {
        System.out.print(".");

    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        System.out.println("Done");
        System.out.println("Restored Ended: " + storeName + ":" + totalRestored);

    }

   
}