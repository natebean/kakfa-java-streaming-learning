package com.natebean;

import static org.assertj.core.api.Assertions.assertThat;

import com.natebean.models.GapLog;
import com.natebean.models.GapLogProductionLogSplitRecord;
import com.natebean.models.ProductionLog;

import org.junit.Test;

public class GapProductionLogSplitRecordTest {

    @Test
    public void noProductionLogTest() {
        GapLogProductionLogSplitRecord glpl = new GapLogProductionLogSplitRecord(new GapLog(70, 1, 1, 5, 10), null);
        assertThat(glpl.startTime).isEqualTo(glpl.gapLogRecord.startTime);
        assertThat(glpl.endTime).isEqualTo(glpl.gapLogRecord.endTime);
    }

    @Test
    public void gapLogWithinAProductionLog() {
        GapLogProductionLogSplitRecord glpl = new GapLogProductionLogSplitRecord(new GapLog(70, 1, 1, 5, 10),
                new ProductionLog(70, 1, 2, 1, 20));
        assertThat(glpl.startTime).isEqualTo(glpl.gapLogRecord.startTime);
        assertThat(glpl.endTime).isEqualTo(glpl.gapLogRecord.endTime);
        assertThat(glpl.duration).isEqualTo(5L);
    }

    @Test
    public void gapLogSplitAcrossProductionLog() {
        GapLogProductionLogSplitRecord glpl = new GapLogProductionLogSplitRecord(new GapLog(70, 1, 1, 1, 10),
                new ProductionLog(70, 1, 2, 5, 15));
        assertThat(glpl.startTime).isEqualTo(glpl.productionLogRecord.startTime);
        assertThat(glpl.endTime).isEqualTo(glpl.gapLogRecord.endTime);
        assertThat(glpl.duration).isEqualTo(5L);
        assertThat(glpl.completeRecord).isEqualTo(false);
    }

}