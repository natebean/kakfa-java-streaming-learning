package com.natebean.singlestream;

import java.util.ArrayList;
import java.util.List;

import com.natebean.singlestream.GapProductionLogSplitStream;
import com.natebean.models.GapLog;
import com.natebean.models.GapLogProductionLogSplitRecord;
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

        if (kvStore.approximateNumEntries() == 0) {
            System.out.println("Store is empty");
        }
    }

    @Override
    public void close() {

    }

    @Override
    public Iterable<GapLogProductionLogSplitRecord> transform(GapLog gl) {

        String startRange = String.format("%d:%d", gl.sidId, gl.sysId);
        String endRange = String.format("%d:%d", gl.sidId, gl.sysId + 1);

        List<GapLogProductionLogSplitRecord> results = new ArrayList<>();
        JSONSerde<ProductionLog> js = new JSONSerde<>();

        KeyValueIterator<String, ValueAndTimestamp<String>> range = kvStore.range(startRange, endRange);
        while (range.hasNext()) {
            KeyValue<String, ValueAndTimestamp<String>> productionLogMessage = range.next();
            ValueAndTimestamp<String> plvt = productionLogMessage.value;
            ProductionLog pl = js.deserialize("nop", plvt.value().toString().getBytes());
            if (gl.startTime < pl.endTime && gl.endTime > pl.startTime && gl.sidId == pl.sidId
                    && gl.sysId == pl.sysId) {
                        //TODO why do we need to check sidID? and sysId?
                results.add(new GapLogProductionLogSplitRecord(gl, pl));
            }
        }

        boolean needMoreChecking = false;

        if (results.size() == 0) {
            /* Missed the Production Log Entry */
            System.out.println("Missing PLog");
            results.add(new GapLogProductionLogSplitRecord(gl, null));
        } else if (results.size() == 1) {
            /* This is the golden path, 98% of the time */
            needMoreChecking = !results.get(0).completeRecord;
        } else {
            needMoreChecking = true;
        }

        if (needMoreChecking) {
            System.out.println("needMoreChecking");
            if (!capturedAllDurations(results, gl))
                fillGaps(results, gl);
            // TODO handle overlapping entries
        }

        range.close();
        js.close();
        return results;
    }

    public static boolean capturedAllDurations(List<GapLogProductionLogSplitRecord> results, GapLog gl) {

        long capturedDuration = results.stream().map(v -> v.duration).reduce(0L, Long::sum);

        return (capturedDuration == gl.endTime - gl.startTime);

    }

    /* Make sure results are contiguous and preserve the full duration of the gaplog record */
    static void fillGaps(List<GapLogProductionLogSplitRecord> results, GapLog gl) {
        //TODO stub

    }

}