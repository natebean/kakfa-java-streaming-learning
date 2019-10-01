package com.natebean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.natebean.models.GapLog;
import com.natebean.models.GapLogProductionLogSplitRecord;
import com.natebean.models.JSONSerde;
import com.natebean.models.JSONSerdeCompatible;
import com.natebean.models.ProductionLog;
import com.natebean.producers.GapLogProducer;
import com.natebean.producers.ProductionLogProducer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class GapProductionLogSplitStreamTest {

        private TopologyTestDriver testDriver;

        private StringDeserializer stringDeserializer = new StringDeserializer();
        private ConsumerRecordFactory<String, ProductionLog> productionLogFactory = new ConsumerRecordFactory<>(
                        new StringSerializer(), new JSONSerde<ProductionLog>());
        private ConsumerRecordFactory<String, GapLog> gapLogFactory = new ConsumerRecordFactory<>(
                        new StringSerializer(), new JSONSerde<GapLog>());

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
                        System.out.println("Ignoring exception, test failing in Windows due this exception:"
                                        + e.getLocalizedMessage());
                }
        }

        private ConsumerRecord<byte[], byte[]> createProductionLog(String key, ProductionLog value) {
                return productionLogFactory.create(ProductionLogProducer.SIMPLE_JSON_TOPIC, value.getKafkaKey(), value);
        }

        private ConsumerRecord<byte[], byte[]> createGapLog(String key, GapLog value) {
                return gapLogFactory.create(GapLogProducer.SIMPLE_JSON_TOPIC, value.getKafkaKey(), value);
        }

        private static <T extends JSONSerdeCompatible> void assertStringsEquals(T t1, T t2) {

                JSONSerde<T> js = new JSONSerde<>();
                String answerString = js.toJSONString(t1);
                String sutString = js.toJSONString(t2);
                js.close();
                assertEquals(answerString, sutString);
        }

        private static <T extends JSONSerdeCompatible> String jsonSerdeCompatibleObjectToString(T t1) {

                JSONSerde<T> js = new JSONSerde<>();
                String answerString = js.toJSONString(t1);
                js.close();
                return answerString;
        }

        // private Map<String, String> getKeyAndValueStringMap() {
        // final Map<String, String> output = new HashMap<>();
        // ProducerRecord<String, GapLogProductionLogSplitRecord> outputRow;
        // while ((outputRow =
        // testDriver.readOutput(GapProductionLogSplitStream.STREAM_OUTPUT,
        // stringDeserializer,
        // new JSONSerde<>())) != null) {
        // output.put(outputRow.key(), outputRow.value().toString());
        // }
        // return output;
        // }

        private ProducerRecord<String, GapLogProductionLogSplitRecord> readOutput() {
                return testDriver.readOutput(GapProductionLogSplitStream.STREAM_OUTPUT, stringDeserializer,
                                new JSONSerde<>());
        }

        private List<String> getOutputStringList() {
                final List<String> output = new ArrayList<>();
                ProducerRecord<String, GapLogProductionLogSplitRecord> outputRow;
                while ((outputRow = readOutput()) != null) {
                        output.add(jsonSerdeCompatibleObjectToString(outputRow.value()));
                }
                return output;
        }

        @Test
        public void testKTableStartsEmpty() {
                KeyValueStore<Object, Object> kvstore = testDriver
                                .getKeyValueStore(GapProductionLogSplitStream.STATE_STORE_NAME);
                assertThat(kvstore.approximateNumEntries()).isEqualTo(0L);

        }

        @Test
        public void testOneRecordKTable() {

                ConsumerRecord<byte[], byte[]> record = productionLogFactory.create(
                                ProductionLogProducer.SIMPLE_JSON_TOPIC, "any", new ProductionLog(70, 1, 1, 1L, 5L));

                testDriver.pipeInput(record);

                KeyValueStore<Object, Object> kvstore = testDriver
                                .getKeyValueStore(GapProductionLogSplitStream.STATE_STORE_NAME);
                assertThat(kvstore.approximateNumEntries()).isEqualTo(1L);

        }

        @Test
        public void testGapLogInProductionLog() {

                ProductionLog pl = new ProductionLog(70, 1, 1, 1L, 50L);
                GapLog gl = new GapLog(70, 1, 10, 1L, 3L);
                GapLogProductionLogSplitRecord answer = new GapLogProductionLogSplitRecord(gl, pl);

                ConsumerRecord<byte[], byte[]> plRecord = createProductionLog(pl.getKafkaKey(), pl);
                ConsumerRecord<byte[], byte[]> glRecord = createGapLog(gl.getKafkaKey(), gl);

                testDriver.pipeInput(plRecord);
                testDriver.pipeInput(glRecord);

                ProducerRecord<String, GapLogProductionLogSplitRecord> producerRecord = readOutput();

                assertThat(producerRecord.key()).isEqualTo(gl.getKafkaKey());
                assertStringsEquals(answer, producerRecord.value());
                assertThat(readOutput()).isNull();

        }

        @Test
        public void testGapLogNoProductionLog() {

                GapLog gl = new GapLog(70, 1, 10, 1L, 3L);
                GapLogProductionLogSplitRecord answer = new GapLogProductionLogSplitRecord(gl, null);

                ConsumerRecord<byte[], byte[]> glRecord = createGapLog(gl.getKafkaKey(), gl);

                testDriver.pipeInput(glRecord);

                ProducerRecord<String, GapLogProductionLogSplitRecord> producerRecord = readOutput();

                assertThat(producerRecord.key()).isEqualTo(gl.getKafkaKey());
                assertStringsEquals(answer, producerRecord.value());
                assertThat(readOutput()).isNull();

        }

        @Test
        public void testGapLogSimpleSplit() {

                List<ProductionLog> plList = new ArrayList<>();
                plList.add(new ProductionLog(70, 1, 1, 1L, 10L));
                plList.add(new ProductionLog(70, 1, 2, 10L, 20L));

                GapLog gl = new GapLog(70, 1, 10, 5L, 15L);
                ConsumerRecord<byte[], byte[]> glRecord = createGapLog(gl.getKafkaKey(), gl);

                List<KeyValue<String, ProductionLog>> inputValues = plList.stream()
                                .map(v -> new KeyValue<String, ProductionLog>(v.getKafkaKey(), v))
                                .collect(Collectors.toList());

                testDriver.pipeInput(productionLogFactory.create(ProductionLogProducer.SIMPLE_JSON_TOPIC, inputValues));

                testDriver.pipeInput(glRecord);

                List<String> producerRecords = getOutputStringList();

                List<String> expected = plList.stream()
                                .map(v -> jsonSerdeCompatibleObjectToString(new GapLogProductionLogSplitRecord(gl, v)))
                                .collect(Collectors.toList());

                assertThat(producerRecords).containsAll(expected);

        }

        @Test
        public void testGapLogThreeSplitWithOverLap() {

                List<ProductionLog> plList = new ArrayList<>();
                plList.add(new ProductionLog(70, 1, 1, 1L, 10L));
                plList.add(new ProductionLog(70, 1, 2, 10L, 20L));
                plList.add(new ProductionLog(70, 1, 3, 25L, 40L));

                GapLog gl = new GapLog(70, 1, 10, 5L, 35L);
                ConsumerRecord<byte[], byte[]> glRecord = createGapLog(gl.getKafkaKey(), gl);

                List<KeyValue<String, ProductionLog>> inputValues = plList.stream()
                                .map(v -> new KeyValue<String, ProductionLog>(v.getKafkaKey(), v))
                                .collect(Collectors.toList());

                testDriver.pipeInput(productionLogFactory.create(ProductionLogProducer.SIMPLE_JSON_TOPIC, inputValues));

                testDriver.pipeInput(glRecord);

                List<String> producerRecords = getOutputStringList();

                List<String> expected = plList.stream()
                                .map(v -> jsonSerdeCompatibleObjectToString(new GapLogProductionLogSplitRecord(gl, v)))
                                .collect(Collectors.toList());

                assertThat(producerRecords).containsAll(expected);

        }

        @Test
        @Ignore
        public void testGapLogWithGapInProductionLog() {

                List<ProductionLog> plList = new ArrayList<>();
                plList.add(new ProductionLog(70, 1, 1, 1L, 10L));
                plList.add(new ProductionLog(70, 1, 3, 25L, 40L));

                GapLog gl = new GapLog(70, 1, 10, 5L, 35L);
                ConsumerRecord<byte[], byte[]> glRecord = createGapLog(gl.getKafkaKey(), gl);

                List<KeyValue<String, ProductionLog>> inputValues = plList.stream()
                                .map(v -> new KeyValue<String, ProductionLog>(v.getKafkaKey(), v))
                                .collect(Collectors.toList());

                testDriver.pipeInput(productionLogFactory.create(ProductionLogProducer.SIMPLE_JSON_TOPIC, inputValues));

                testDriver.pipeInput(glRecord);

                List<String> producerRecords = getOutputStringList();

                List<String> expected = plList.stream()
                                .map(v -> jsonSerdeCompatibleObjectToString(new GapLogProductionLogSplitRecord(gl, v)))
                                .collect(Collectors.toList());
                expected.add(jsonSerdeCompatibleObjectToString(
                                new GapLogProductionLogSplitRecord(gl, new ProductionLog(70, 1, -1, 10L, 25L))));

                assertThat(producerRecords).containsAll(expected);

        }

}

/*
 * 
 * private ProducerRecord<String, Long> readOutput() { return
 * testDriver.readOutput(WordCountLambdaExample.outputTopic, stringDeserializer,
 * longDeserializer); }
 * 
 * private Map<String, Long> getOutputList() { final Map<String, Long> output =
 * new HashMap<>(); ProducerRecord<String, Long> outputRow; while ((outputRow =
 * readOutput()) != null) { output.put(outputRow.key(), outputRow.value()); }
 * return output; }
 * 
 * @Test public void testOneWord() { final String nullKey = null; // Feed word
 * "Hello" to inputTopic and no kafka key, timestamp is irrelevant in // this
 * case
 * testDriver.pipeInput(recordFactory.create(WordCountLambdaExample.inputTopic,
 * nullKey, "Hello", 1L)); // Read and validate output final
 * ProducerRecord<String, Long> output = readOutput();
 * OutputVerifier.compareKeyValue(output, "hello", 1L); // No more output in
 * 
 * topic assertThat(readOutput()).isNull(); }
 * 
 * @Test public void shouldCountWords() { final List<String> inputValues =
 * Arrays.asList("Hello Kafka Streams", "All streams lead to Kafka",
 * "Join Kafka Summit", "И теперь пошли русские слова"); final Map<String, Long>
 * expectedWordCounts = new HashMap<>(); expectedWordCounts.put("hello", 1L);
 * expectedWordCounts.put("all", 1L); expectedWordCounts.put("streams", 2L);
 * expectedWordCounts.put("lead", 1L); expectedWordCounts.put("to", 1L);
 * expectedWordCounts.put("join", 1L); expectedWordCounts.put("kafka", 3L);
 * expectedWordCounts.put("summit", 1L); expectedWordCounts.put("и", 1L);
 * expectedWordCounts.put("теперь", 1L); expectedWordCounts.put("пошли", 1L);
 * expectedWordCounts.put("русские", 1L); expectedWordCounts.put("слова", 1L);
 * 
 * final List<KeyValue<String, String>> records = inputValues.stream() .map(v ->
 * new KeyValue<String, String>(null, v)).collect(Collectors.toList());
 * testDriver.pipeInput(recordFactory.create(WordCountLambdaExample.inputTopic,
 * records, 1L, 100L));
 * 
 * final Map<String, Long> actualWordCounts = getOutputList();
 * assertThat(actualWordCounts).containsAllEntriesOf(expectedWordCounts); }
 * 
 */
