package com.natebean.producers;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Random;

import com.natebean.models.GapLog;

public class GapLogFactory {

    public static GapLog getNextGapLogRecord(int sidId, int sysId, int productionId, long lastEndTime) {
        Random rand = new Random();
        int duration = rand.nextInt(10);
        LocalDateTime start = Instant.ofEpochMilli(lastEndTime * 1000).atZone(ZoneId.systemDefault()).toLocalDateTime();
        LocalDateTime end = start.plusMinutes(duration);
        long startTime = start.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
        long endTime = end.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();

        return new GapLog(sidId, sysId, productionId, startTime, endTime);

    }
}