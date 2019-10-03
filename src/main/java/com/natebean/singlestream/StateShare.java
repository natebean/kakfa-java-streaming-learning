package com.natebean.singlestream;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class StateShare {

    ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalStore;

    synchronized public void setGlobalState(ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalState) {

        this.globalStore = globalState;
        // notifyAll();

    }

    synchronized public ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> getGlobalState() {

        // try {
        //     wait();
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
        return this.globalStore;

    }

}