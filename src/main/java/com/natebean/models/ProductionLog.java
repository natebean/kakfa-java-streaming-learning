package com.natebean.models;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Random;

public class ProductionLog implements JSONSerdeCompatible {
    public int sidId;
    public int sysId;
    public int productionId;
    public long startTime;
    public long endTime;

    public ProductionLog(){
        super();
    }

    public ProductionLog(int sidId, int sysId, int productionId) {
        this.sidId = sidId;
        this.sysId = sysId;
        this.productionId = productionId;

        Random rand = new Random();
        int month = rand.nextInt(12) + 1;
        int day = rand.nextInt(28) + 1;
        int duration = rand.nextInt(30);
        LocalDate start = LocalDate.of(2019, month, day);
        LocalDate end = start.plusDays(duration);
        this.startTime = start.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        this.endTime = end.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();

    }
}
