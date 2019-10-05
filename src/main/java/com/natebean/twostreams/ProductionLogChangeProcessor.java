package com.natebean.twostreams;

import com.natebean.models.ProductionLog;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class ProductionLogChangeProcessor implements Processor<String, ProductionLog> {

    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> productionLogGlobalState;

    @Override
    public void init(ProcessorContext context) {

    }

    public ProductionLogChangeProcessor(
            ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> productionLogGlobalState) {
        this.productionLogGlobalState = productionLogGlobalState;
    }

    @Override
    public void process(String key, ProductionLog value) {
        // System.out.println("Change Stream: " + key + " : " + productionLogGlobalState.approximateNumEntries());
        System.out.println("*");
        // TODO confirm the record is in global State, if not wait and try again?
        // punctuate maybe? If find effected records and send back thru
        // gap-log-json
        // ?? Can we use the split table for reference
        // Ktable for lookup?  How far back? 
    }

    @Override
    public void close() {

    }

}
