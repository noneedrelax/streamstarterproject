package com.tony.test.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FavColorStream {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger("StreamApp");
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        // stream config
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color-4");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // consumer config
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        // get k stream from topic
        String topic = "fav-color-input-2";
        String intermidateTopic = "fav-color-intermidate-2";
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String, String> kstream = sb.stream(topic);

        // get the color, DQ check and filter color, split by comma, write to kafka broker as intermmidate.
        kstream.mapValues(value->value.toLowerCase())
                // get comma an filter out the one with wrong format
                .filter((key,tuple)->tuple.split(",").length == 2)
                // get the key
                .map((key, tuple)->new KeyValue(tuple.split(",")[0], tuple.split(",")[1]))
                // filter out the color
                .filter((key, value)->(value.equals("red")||value.equals("green")||value.equals("blue")))
                // move to kafka for ktable convert
                .to(intermidateTopic);

        KTable<String,String> ktable = sb.table(intermidateTopic);

        // ktable to agg the color and counts
        KTable<String, Long> result = ktable.groupBy((k, v) -> KeyValue.pair(v,v)).count(Materialized.as("colorCount"));
        String resultTopic = "fav-color-output";
        result.toStream().to(resultTopic);

        KafkaStreams myWorkflow = new KafkaStreams(sb.build(), properties);
        myWorkflow.start();

        logger.info(myWorkflow.toString());
        // gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(myWorkflow::close));

    }
}
