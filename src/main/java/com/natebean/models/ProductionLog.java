package com.natebean.models;

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
}
