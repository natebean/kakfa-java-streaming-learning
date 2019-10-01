package com.natebean;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import com.natebean.models.GapLog;
import com.natebean.models.GapLogProductionLogSplitRecord;
import com.natebean.models.ProductionLog;
import com.natebean.transformers.GapProductionLogSplitTransformer;

import org.junit.Ignore;
import org.junit.Test;

public class GapProductionLogSplitTransformerTest {

    @Test
    public void capturedDurationHaveItAll() {
        List<GapLogProductionLogSplitRecord> splitRec = new ArrayList<>();
        GapLog gl = new GapLog(70, 1, 1, 5L, 7L);
        ProductionLog pl = new ProductionLog(70, 1, 1, 1L, 10L);
        splitRec.add(new GapLogProductionLogSplitRecord(gl, pl));

        assertThat(GapProductionLogSplitTransformer.capturedAllDurations(splitRec, gl)).isTrue();

    }

    @Test
    public void capturedDurationFromASplit() {
        List<GapLogProductionLogSplitRecord> splitRec = new ArrayList<>();
        GapLog gl = new GapLog(70, 1, 1, 5L, 70L);
        ProductionLog pl = new ProductionLog(70, 1, 1, 1L, 10L);
        splitRec.add(new GapLogProductionLogSplitRecord(gl, pl));

        assertThat(GapProductionLogSplitTransformer.capturedAllDurations(splitRec, gl)).isFalse();

    }

    @Test
    @Ignore
    public void fillGapLeading() {
        List<GapLogProductionLogSplitRecord> splitRec = new ArrayList<>();
        GapLog gl = new GapLog(70, 1, 1, 5L, 70L);
        ProductionLog pl = new ProductionLog(70, 1, 1, 1L, 10L);
        splitRec.add(new GapLogProductionLogSplitRecord(gl, pl));

        assertThat(GapProductionLogSplitTransformer.capturedAllDurations(splitRec, gl)).isFalse();

    }

}