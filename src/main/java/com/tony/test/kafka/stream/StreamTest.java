package com.tony.test.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class StreamTest {


    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger("StreamApp");
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";

        // stream config
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // consumer config
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamTest().getStreamsBuilder();

        KafkaStreams stream = new KafkaStreams(builder.build(),properties);
        stream.start();
        // print topology
        logger.info(stream.toString());
        // gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

    public StreamsBuilder getStreamsBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        //  1 . stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        // 2. map value
        KTable<String, Long> table = wordCountInput.mapValues(value -> value.toLowerCase())
                // 3. flat map
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                // 4. value to key
                .selectKey((key, value) -> value)
                // 5. group by key before agg
                .groupByKey()
                // 6. count occurance
                .count(Materialized.as("counts-store"));
        // 7. write result to kafka
        table.toStream().to("word-count-topic", Produced.with(Serdes.String(), Serdes.Long()));
        return builder;
    }
}
