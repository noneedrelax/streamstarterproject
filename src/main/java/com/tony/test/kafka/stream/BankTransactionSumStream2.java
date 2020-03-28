package com.tony.test.kafka.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BankTransactionSumStream2 {
    static Gson gson = new Gson();
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(BankTransactionSumStream2.class);
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        // stream config
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream2");
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String(
        // consumer config).getClass());
        ////        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        //exactly once config
        properties.put(StreamsConfig.EXACTLY_ONCE, true);

        // get k stream from topic
        String topic = "transaction-history";
//        String intermidateTopic = "transaction-history-intermidate";
        String outputTopic = "transaction-history-output";
        StreamsBuilder sb = new StreamsBuilder();


        KStream<String, Long> kstream = sb.stream(topic, Consumed.with(Serdes.String(),Serdes.String()))
                .selectKey((k,v)->"haha")
                .mapValues((v)->19L);
//        kstream.pr

        // first parse value by the key, and do aggration, and then save the sum back to the table for further processing
        // here do we have to map things twice? one time for key one time for value? then we have to do parsing twice?
        KGroupedStream<String, Long> groupedStream = kstream.groupByKey();

        KTable<String, Long> ktable = groupedStream.aggregate(
                () -> 0L,
                (aggkey, newValue, aggValue) -> newValue + aggValue,
                Materialized.with(Serdes.String(),Serdes.Long())
        );
        ktable.toStream().print(Printed.toSysOut());
//        KStream<String, Long> ab = ktable.toStream();


        KafkaStreams streams = new KafkaStreams(sb.build(),properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KeyValue<byte[],String> parseJsonToTransactionRecord(String v) {
        UserTransaction record = gson.fromJson(v, UserTransaction.class);
        return new KeyValue(record.name.getBytes(), record.amount+"");
    }
}
