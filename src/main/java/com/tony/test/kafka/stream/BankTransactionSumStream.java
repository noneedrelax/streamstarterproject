package com.tony.test.kafka.stream;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BankTransactionSumStream {
    static Gson gson = new Gson();
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(BankTransactionSumStream.class);
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        // stream config
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, BankTransactionSumStream.class.getName());

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        // consumer config
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        //exactly once config
        properties.put(StreamsConfig.EXACTLY_ONCE, true);

        // get k stream from topic
        String topic = "transaction-history";
//        String intermidateTopic = "transaction-history-intermidate";
        String outputTopic = "transaction-history-output";
        StreamsBuilder sb = new StreamsBuilder();


        KStream<String, Long> kstream = sb.stream(topic,Consumed.with(Serdes.String(),Serdes.String()))
            .map((k,v)->parseJsonToTransactionRecord(v));


        // first parse value by the key, and do aggration, and then save the sum back to the table for further processing
        // here do we have to map things twice? one time for key one time for value? then we have to do parsing twice?
        KGroupedStream<String, Long> groupedStream = kstream.groupByKey();

        KTable<String, Long> ktable = groupedStream.aggregate(
                () -> 0L,
               (aggkey, newValue, aggValue) -> newValue + aggValue,
                Materialized.with(Serdes.String(),Serdes.Long()) // important, you must add this line here or else it will fail!
        );
        ktable.toStream()
                .through(outputTopic, Produced.with(Serdes.String(),Serdes.Long()))
                .print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(sb.build(),properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KeyValue<String,Long> parseJsonToTransactionRecord(String v) {
        UserTransaction record = gson.fromJson(v, UserTransaction.class);
        return new KeyValue<>(record.name, new Long(record.amount));
    }
}
