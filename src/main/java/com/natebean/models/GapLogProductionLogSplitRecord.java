package com.natebean.models;

import com.natebean.models.GapLog;
import com.natebean.models.JSONSerdeCompatible;
import com.natebean.models.ProductionLog;

public class GapLogProductionLogSplitRecord implements JSONSerdeCompatible {

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
