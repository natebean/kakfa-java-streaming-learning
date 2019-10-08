package com.natebean.topology;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class StateShare {

    ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalStore;

    synchronized public void setGlobalState(ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalState) {

        this.globalStore = globalState;

    }

    synchronized public ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> getGlobalState() {

        return this.globalStore;

    }

}