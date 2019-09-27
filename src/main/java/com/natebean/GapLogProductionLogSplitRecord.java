package com.natebean;

import com.natebean.models.GapLog;
import com.natebean.models.JSONSerdeCompatible;
import com.natebean.models.ProductionLog;

public class GapLogProductionLogSplitRecord implements JSONSerdeCompatible {
    // public int sidId;
    // public int sysId;
    // public int gapLogId;
    // public long gapLogStartTime;
    // public long gapLogEndTime;
    // public int productionLogId;
    // public long productionLogStartTime;
    // public long productionLogEndTime;

    public GapLog gapLogRecord;
    public ProductionLog productionLogRecord;

    public GapLogProductionLogSplitRecord(){
        super();
    }

    public GapLogProductionLogSplitRecord( GapLog gapLog, ProductionLog productionLog){
        this.gapLogRecord = gapLog;
        this.productionLogRecord = productionLog;
    }

}
