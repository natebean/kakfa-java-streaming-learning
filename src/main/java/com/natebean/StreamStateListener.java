package com.natebean;

import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public class StreamStateListener implements StateListener {

    /**
     * Called when state changes.
     *
     * @param newState new state
     * @param oldState previous state
     */
    public void onChange(final State newState, final State oldState){
        System.out.println(oldState.toString() + " -> " + newState.toString());
    }
}