package com.natebean.processors;

import com.natebean.models.ProductionLog;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class PrintProcessor<T> implements Processor<String, T> {

    private String prefix;

    @Override
    public void init(ProcessorContext context) {
        // TODO Auto-generated method stub

    }

    public PrintProcessor(String prefix){
        this.prefix = prefix;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void process(String key, T value) {
        System.out.println(prefix + ": " + key);

    }

}

