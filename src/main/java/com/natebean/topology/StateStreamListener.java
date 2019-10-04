package com.natebean.topology;

import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;

public class StateStreamListener implements StateListener {

    public StateStreamListener() {
    }

    @Override
    public void onChange(State newState, State oldState) {
        System.out.println(oldState + "->" + newState);
        // if (newState == State.RUNNING){
        // System.out.println("Global State after Created" +
        // globalState.approximateNumEntries());
        // }

    }

}
