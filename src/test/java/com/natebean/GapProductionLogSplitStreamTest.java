package com.natebean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.natebean.models.GapLog;
import com.natebean.models.GapLogProductionLogSplitRecord;
import com.natebean.models.JSONSerde;
import com.natebean.models.ProductionLog;
import com.natebean.producers.GapLogProducer;
import com.natebean.producers.ProductionLogProducer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GapProductionLogSplitStreamTest {

    private TopologyTestDriver testDriver;

    private StringDeserializer stringDeserializer = new StringDeserializer();
    private ConsumerRecordFactory<String, ProductionLog> productionLogFactory = new ConsumerRecordFactory<>(
            new StringSerializer(), new JSONSerde<ProductionLog>());
    private ConsumerRecordFactory<String, GapLog> gapLogFactory = new ConsumerRecordFactory<>(new StringSerializer(),
            new JSONSerde<GapLog>());

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        // Create Actual Stream Processing pipeline
        GapProductionLogSplitStream.createStream(builder);
        testDriver = new TopologyTestDriver(builder.build(),
                GapProductionLogSplitStream.getStreamsConfiguration("localhost:9092", true));
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when
            // executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println(
                    "Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testKTableStartsEmpty() {
        KeyValueStore<Object, Object> kvstore = testDriver
                .getKeyValueStore(GapProductionLogSplitStream.STATE_STORE_NAME);
        assertThat(kvstore.approximateNumEntries(), is(0L));

    }

    @Test
    public void testOneRecordKTable() {

        ConsumerRecord<byte[], byte[]> record = productionLogFactory.create(ProductionLogProducer.SIMPLE_JSON_TOPIC,
                "1:1:1", new ProductionLog(70, 1, 1, 1L, 5L));

        testDriver.pipeInput(record);

        KeyValueStore<Object, Object> kvstore = testDriver
                .getKeyValueStore(GapProductionLogSplitStream.STATE_STORE_NAME);
        assertThat(kvstore.approximateNumEntries(), is(1L));

    }

    @Test
    public void testReadOutput() {

        //TODO build key builder
        //TODO parent class for gap and production log ?
        ConsumerRecord<byte[], byte[]> plRecord = productionLogFactory.create(ProductionLogProducer.SIMPLE_JSON_TOPIC,
                "70:1:1", new ProductionLog(70, 1, 1, 1L, 50L));
        ConsumerRecord<byte[], byte[]> glRecord = gapLogFactory.create(GapLogProducer.SIMPLE_JSON_TOPIC, "70:1:10",
                new GapLog(70, 1, 1, 1L, 3L));

        testDriver.pipeInput(plRecord);
        testDriver.pipeInput(glRecord);

        ProducerRecord<String, GapLogProductionLogSplitRecord> producerRecord = testDriver.readOutput(
                GapProductionLogSplitStream.STREAM_OUTPUT, stringDeserializer,
                new JSONSerde<GapLogProductionLogSplitRecord>());

        assertThat(producerRecord.key(), is("70:1:10"));

    }

}


/*

    private ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput(WordCountLambdaExample.outputTopic, stringDeserializer, longDeserializer);
    }

    private Map<String, Long> getOutputList() {
        final Map<String, Long> output = new HashMap<>();
        ProducerRecord<String, Long> outputRow;
        while ((outputRow = readOutput()) != null) {
            output.put(outputRow.key(), outputRow.value());
        }
        return output;
    }

    @Test
    public void testOneWord() {
        final String nullKey = null;
        // Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in
        // this case
        testDriver.pipeInput(recordFactory.create(WordCountLambdaExample.inputTopic, nullKey, "Hello", 1L));
        // Read and validate output
        final ProducerRecord<String, Long> output = readOutput();
        OutputVerifier.compareKeyValue(output, "hello", 1L);
        // No more output in topic
        assertThat(readOutput()).isNull();
    }


    @Test
    public void shouldCountWords() {
        final List<String> inputValues = Arrays.asList("Hello Kafka Streams", "All streams lead to Kafka",
                "Join Kafka Summit", "И теперь пошли русские слова");
        final Map<String, Long> expectedWordCounts = new HashMap<>();
        expectedWordCounts.put("hello", 1L);
        expectedWordCounts.put("all", 1L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("lead", 1L);
        expectedWordCounts.put("to", 1L);
        expectedWordCounts.put("join", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("summit", 1L);
        expectedWordCounts.put("и", 1L);
        expectedWordCounts.put("теперь", 1L);
        expectedWordCounts.put("пошли", 1L);
        expectedWordCounts.put("русские", 1L);
        expectedWordCounts.put("слова", 1L);

        final List<KeyValue<String, String>> records = inputValues.stream()
                .map(v -> new KeyValue<String, String>(null, v)).collect(Collectors.toList());
        testDriver.pipeInput(recordFactory.create(WordCountLambdaExample.inputTopic, records, 1L, 100L));

        final Map<String, Long> actualWordCounts = getOutputList();
        assertThat(actualWordCounts).containsAllEntriesOf(expectedWordCounts);
    }

 */
