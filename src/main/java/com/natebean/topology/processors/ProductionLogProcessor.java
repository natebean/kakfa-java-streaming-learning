package com.natebean.topology.processors;

import com.natebean.models.ProductionLog;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ProductionLogProcessor implements Processor<String, ProductionLog> {

    private String prefix;
    private KafkaProducer<String, ProductionLog> productionLogProducer;

    @Override
    public void init(ProcessorContext context) {

    }

    public ProductionLogProcessor(String prefix, KafkaProducer<String, ProductionLog> productionLogProducer) {
        this.prefix = prefix;
        this.productionLogProducer = productionLogProducer;
    }

    @Override
    public void process(String key, ProductionLog value) {
        System.out.println(prefix + ": " + key);
        //TODO update global state
        String PRODUCTION_LOG_CHANGED = "production-log-changed-json";
        ProducerRecord<String, ProductionLog> record = new ProducerRecord<>(PRODUCTION_LOG_CHANGED, key, value);
        productionLogProducer.send(record);
        productionLogProducer.flush();
    }

    @Override
    public void close() {

    }

}
