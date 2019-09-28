package com.natebean.justincase;

import com.natebean.GapProductionLogSplitStream;
import com.natebean.models.GapLog;
import com.natebean.models.ProductionLog;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/*  Not used at this point  9/27/2019 */
public class GapLogProductionLogProcessor implements Processor<String, GapLog> {

    // private ProcessorContext context;
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<ProductionLog>> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // this.context = context;
        kvStore = (ReadOnlyKeyValueStore<String, ValueAndTimestamp<ProductionLog>>) context.getStateStore(GapProductionLogSplitStream.STATE_STORE_NAME);

    }

    @Override
    public void process(String key, GapLog value) {
        System.out.println("processing gap log: " + key + ":" + value);

        ValueAndTimestamp<ProductionLog> found = kvStore.get(key);
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