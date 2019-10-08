package com.natebean.models;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class GapLog implements JSONSerdeCompatible {
    public int sidId;
    public int sysId;
    public int gapLogId;
    public long startTime;
    public long endTime;

    public GapLog() {
        super();
    }

    public GapLog(int sidId, int sysId, int gapLogId, long startTime, long endTime) {
        this.sidId = sidId;
        this.sysId = sysId;
        this.gapLogId = gapLogId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @JsonIgnore
    public String getKafkaKey() {
        return sidId + ":" + sysId + ":" + gapLogId;

    }
}
