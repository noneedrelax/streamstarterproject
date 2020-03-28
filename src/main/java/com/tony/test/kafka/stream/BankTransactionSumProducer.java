package com.tony.test.kafka.stream;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.*;

public class BankTransactionSumProducer {
    static int maxInt = 1000;
    static Gson gson = new Gson();
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    public static void main(String[] args) throws InterruptedException {
        // initialize the kafka server instance. 
        final Logger logger = LoggerFactory.getLogger(BankTransactionSumProducer.class);
        // create producer properties
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        int frequencyWaitSec = 1;
        int generateBatch = 10;
        int loopTimes = 10;
        String topic = "transaction-history";
        
        // initialize the arrays and random
        Random random = new Random();

        List<String> list = Arrays.asList("Tony", "Bob", "Alice", "Jack", "Mary", "Bill");

        for (int i = 0; i < loopTimes; i++) {
            // produce message now!
            for (int j = 0; j < generateBatch; j++) {
                producer.send(new ProducerRecord<>(topic, getNextRandomTransaction(random,list)),
                        (meta,e)-> logger.info("sent {}", meta.serializedValueSize()));
            }
            Thread.sleep(frequencyWaitSec*1000L);
        }
    }

    private static String getNextRandomTransaction(Random secureRandom, List<String> list) {
        int amount = secureRandom.nextInt(maxInt)+1;
        String name = list.get(secureRandom.nextInt(list.size()));
        long now = System.currentTimeMillis();
        UserTransaction data = new UserTransaction(name, amount, sdf.format(now));
        return gson.toJson(data);
    }


}
