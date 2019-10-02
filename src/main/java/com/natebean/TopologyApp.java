package com.natebean;

import com.natebean.processors.ProductionLogProcessor;
import com.natebean.producers.ProductionLogProducer;

import org.apache.kafka.streams.Topology;

public class TopologyApp {

    public static void main(String[] args) {

        Topology builder = new Topology();

        // add the source processor node that takes Kafka topic "source-topic" as input
        builder.addSource("Source", ProductionLogProducer.SIMPLE_JSON_TOPIC)

                // add the WordCountProcessor node which takes the source processor as its
                // upstream processor
                .addProcessor("Process", () -> new ProductionLogProcessor(), "Source");

                // add the count store associated with the WordCountProcessor processor
                // .addStateStore(countStoreBuilder, "Process")

                // add the sink processor node that takes Kafka topic "sink-topic" as output
                // and the WordCountProcessor node as its upstream processor
                // .addSink("Sink", "sink-topic", "Process");

    }

}