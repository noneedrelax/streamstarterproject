package com.tony.test.kafka.stream;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.TooManyListenersException;

import static org.junit.Assert.*;

public class StreamTestTest {
    String topic = "word-count-input";
    TopologyTestDriver ttd;
    ConsumerRecordFactory<String,String> crf = new ConsumerRecordFactory(topic,new StringSerializer(),new StringSerializer());
    @org.junit.After
    public void tearDown()  {
        try {
            ttd.close();
        } catch (Exception e){
            try {
                FileUtils.deleteDirectory(new File("C:\\temp\\kafka"));
            } catch (IOException ex) {

            }
        }

    }

    @Before
    public void setup(){
        Properties properties = new Properties();
        String bootStrapServers = "dummy:9092";
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app-junit");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATE_DIR_CONFIG,"C:\\temp\\kafka");
        StreamsBuilder builder = new StreamTest().getStreamsBuilder();
        ttd = new TopologyTestDriver(builder.build(),properties);

    }

    @org.junit.Test
    public void testRecord() {
        ttd.pipeInput(crf.create("Kafka testing kafka"));
        ProducerRecord<String, Long> record = ttd.readOutput("word-count-topic", new StringDeserializer(), new LongDeserializer());
        ProducerRecord<String, Long> record2 = ttd.readOutput("word-count-topic", new StringDeserializer(), new LongDeserializer());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record2.key());
        System.out.println(record2.value());
    }

}