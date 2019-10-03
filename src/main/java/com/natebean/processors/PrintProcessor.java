package com.natebean.processors;

import com.natebean.models.ProductionLog;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class PrintProcessor<T> implements Processor<String, T> {

    private String prefix;
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<ProductionLog>> kvStore;
    private String stateStoreName;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        kvStore = (ReadOnlyKeyValueStore<String, ValueAndTimestamp<ProductionLog>>) context
        .getStateStore(stateStoreName);
    }

    public PrintProcessor(String prefix, String stateStoreName){
        this.prefix = prefix;
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void close() {

    }

    @Override
    public void process(String key, T value) {
        // ValueAndTimestamp<ProductionLog> ans = kvStore.get("1:1:1:2");

        // System.out.println(prefix + ": " + key + " -> " + kvStore.approximateNumEntries() + " --> " + (ans == null));
        System.out.println(prefix + ": " + key + " -> " + kvStore.approximateNumEntries());

    }

}

