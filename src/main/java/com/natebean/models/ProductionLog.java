package com.natebean.models;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ProductionLog implements JSONSerdeCompatible {
    public int sidId;
    public int sysId;
    public int productionId;
    public long startTime;
    public long endTime;

    public ProductionLog() {
        super();
    }

    public ProductionLog(int sidId, int sysId, int productionId, long startTime, long endTime) {
        this.sidId = sidId;
        this.sysId = sysId;
        this.productionId = productionId;
        this.startTime = startTime;
        this.endTime = endTime;

    }

    @JsonIgnore
    public String getKafkaKey(){
        return sidId + ":" + sysId + ":" + productionId;

    }
}
