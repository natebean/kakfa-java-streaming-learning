package com.natebean.producers;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Random;

import com.natebean.models.ProductionLog;

public class ProductionLogFactory {

    public static ProductionLog getNextProductionLogRecord(int sidId, int sysId, int productionId, long lastEndTime) {
        Random rand = new Random();
        int duration = rand.nextInt(5);
        LocalDate start = Instant.ofEpochMilli(lastEndTime * 1000).atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate end = start.plusDays(duration);
        long startTime = start.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        long endTime = end.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();

        return new ProductionLog(sidId, sysId, productionId, startTime, endTime);

    }
}