package com.natebean;

import java.util.ArrayList;
import java.util.List;

import com.natebean.models.GapLog;
import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class GapProductionLogSplitTransformer
        implements ValueTransformer<GapLog, Iterable<GapLogProductionLogSplitRecord>> {

    // private ProcessorContext context;
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // this.context = context;
        kvStore = (ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>) context
                .getStateStore(GapProductionLogSplitStream.STATE_STORE_NAME);

    }

    @Override
    public void close() {

    }

    @Override
    public Iterable<GapLogProductionLogSplitRecord> transform(GapLog gapLogRecord) {

        String startRange = String.format("%d:%d", gapLogRecord.sidId, gapLogRecord.sysId);
        String endRange = String.format("%d:%d", gapLogRecord.sidId, gapLogRecord.sysId + 1);

        List<GapLogProductionLogSplitRecord> results = new ArrayList<>();

        KeyValueIterator<String, ValueAndTimestamp<String>> range = kvStore.range(startRange, endRange);
        while (range.hasNext()) {
            KeyValue<String, ValueAndTimestamp<String>> productionLogMessage = range.next();
            ValueAndTimestamp<String> plvt = productionLogMessage.value;
            JSONSerde<ProductionLog> js = new JSONSerde<>();
            ProductionLog pl = js.deserialize("nop", plvt.value().toString().getBytes());

            if (gapLogRecord.startTime < pl.endTime && gapLogRecord.endTime > pl.startTime
                    && gapLogRecord.sidId == pl.sidId && gapLogRecord.sysId == pl.sysId) {
                results.add(new GapLogProductionLogSplitRecord(gapLogRecord, pl));
            }

        }
        range.close();
        return results;
    }

}