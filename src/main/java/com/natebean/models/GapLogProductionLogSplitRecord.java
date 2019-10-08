package com.natebean.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.natebean.models.GapLog;
import com.natebean.models.JSONSerdeCompatible;
import com.natebean.models.ProductionLog;

public class GapLogProductionLogSplitRecord implements JSONSerdeCompatible {

    public long startTime = 0;
    public long endTime = 0;
    public long duration = 0;
    public GapLog gapLogRecord;
    public ProductionLog productionLogRecord;
    public boolean completeRecord = false;

    public GapLogProductionLogSplitRecord() {
        super();
    }

    public GapLogProductionLogSplitRecord(GapLog gapLog, ProductionLog productionLog) {
        this.gapLogRecord = gapLog;
        this.productionLogRecord = productionLog;
        updateEffectiveStartAndEndTimes();

    }

    @JsonIgnore
    public String getKafkaKey() {
        return gapLogRecord.getKafkaKey() + "::" + productionLogRecord.getKafkaKey();
    }
    
    @JsonIgnore
    private void updateEffectiveStartAndEndTimes() {
        if (productionLogRecord == null) {
            startTime = gapLogRecord.startTime;
            endTime = gapLogRecord.endTime;
        } else {
            startTime = Math.max(gapLogRecord.startTime, productionLogRecord.startTime);
            endTime = Math.min(gapLogRecord.endTime, productionLogRecord.endTime);
        }
        duration = endTime - startTime;
        if (duration == gapLogRecord.endTime - gapLogRecord.startTime)
            completeRecord = true;

    }

}
