package com.natebean.firstapp;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class IntegerProcessor implements Processor<Integer, Integer> {

    // private ProcessorContext context;
    private ReadOnlyKeyValueStore<Integer, ValueAndTimestamp<Integer>> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // this.context = context;
        kvStore = (ReadOnlyKeyValueStore<Integer, ValueAndTimestamp<Integer>>) context.getStateStore(FirstStream.STATE_STORE_NAME);

    }

    @Override
    public void process(Integer key, Integer value) {
        System.out.println("processing: " + key + ":" + value);
        System.out.println("processer readState: " + kvStore.approximateNumEntries());

        ValueAndTimestamp<Integer> found = kvStore.get(key);
        System.out.println("look up value:" + found + ": " + found.value());
        // KeyValueIterator<Integer, Integer> range = kvStore.all();
        // while(range.hasNext()){
        // KeyValue<Integer, Integer> next = range.next();
        // System.out.println("sum for key: " + next.key + ": " + next.value);
        // }

    }

    @Override
    public void close() {

    }

}