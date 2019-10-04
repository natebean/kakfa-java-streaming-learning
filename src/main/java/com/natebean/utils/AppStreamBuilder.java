package com.natebean.utils;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

public interface AppStreamBuilder {

    void createStream(StreamsBuilder builder);

    KafkaStreams buildStream();

    Properties getStreamsConfiguration(String bootstrapServers);

    Properties getStreamsConfiguration(String bootstrapServers, boolean useTempLocation);
}