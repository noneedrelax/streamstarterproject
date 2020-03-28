package com.tony.test.kafka.stream.global;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Properties;

public class UserPurchaseRecordStream {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(UserPurchaseRecordStream.class);
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        // stream config
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UserPurchaseRecordStream.class.getName());

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // consumer config
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        String userTopic = "user-record";
        String purchaseTopic = "purchase-record";
//        String resultTopicInner = "purchase-inner-join";
        String resultTopicLeft = "purchase-left-join";

        //exactly once config
        properties.put(StreamsConfig.EXACTLY_ONCE, true);

        // initialize
        StreamsBuilder sb = new StreamsBuilder();
        Serializer<UserRecord> sr = (s, userRecord) -> 
                (userRecord.name+","+userRecord.gender+","+userRecord.email).getBytes();
        Deserializer<UserRecord> dsr = (s, bytes) -> {
            String[] tokens = new String(bytes).split(",");
            return new UserRecord(tokens[0],tokens[1],tokens[2]);
        };

        Serializer<PurchaseRecord> psr = (s, record)->
                (record.name+","+record.item+","+record.price).getBytes();
        Deserializer<PurchaseRecord> pdsr = (s, bytes) ->{
            String[] tokens = new String(bytes).split(",");
            return new PurchaseRecord(tokens[0],tokens[1], Double.parseDouble(tokens[2]));
        };

        // read user record to global table and deserialize
        GlobalKTable<String, UserRecord> userTable = sb
                .globalTable(userTopic, Consumed.with(Serdes.String(),Serdes.serdeFrom(sr,dsr)));

        // read user purchase record and parse.
        KStream<String, PurchaseRecord> purchaseStream = sb
                .stream(purchaseTopic, Consumed.with(Serdes.String(), Serdes.serdeFrom(psr,pdsr)));

        // do joins
        // this is like select enrichedPurchaseLog() from userPurchase inner jion userTable jon userprofile.key=usertable.key.
        KStream<String, String> data = purchaseStream.leftJoin(
                userTable,
                (key, value) -> key,
                (userPurchase, userRecord) -> enrichedPurchaseLog(userPurchase, userRecord)
                )
                .through(resultTopicLeft)
                .peek((x, y) -> logger.info("message {}, {}", x, y));

        KTable<String, Double> groupedStream = purchaseStream.groupByKey().aggregate(
                () -> 0.0,
                (key, newVal, aggVal) -> newVal.price + aggVal,
                Materialized.with(Serdes.String(), Serdes.Double())
        );
        groupedStream.toStream().leftJoin(
                userTable,
                (key,value)->key,
                (sum, userRecord)-> enrichedPurchaseWithSumLog(sum, userRecord)
        ).print(Printed.toSysOut());


        KafkaStreams app = new KafkaStreams(sb.build(),properties);
        logger.info("App started {} ",app);
        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()->app.close()));
    }

    private static String enrichedPurchaseLog(PurchaseRecord userPurchase, UserRecord userRecord) {
        StringBuilder sb = new StringBuilder();
        sb.append(MessageFormat.format("A user named {0} ",userPurchase.name ));
        if (userRecord==null)
            sb.append("with No additional info ");
        else
            sb.append(MessageFormat.format("gender {0}, email {1} has ", userRecord.gender, userRecord.email));
        sb.append(MessageFormat.format("made a purchase of book: {0} that worth ${1}",userPurchase.item, userPurchase.price));
        return sb.toString();
    }

    private static String enrichedPurchaseWithSumLog(Double amount, UserRecord userRecord) {
        StringBuilder sb = new StringBuilder();
        if (userRecord==null)
            sb.append("A user with No additional info ");
        else{
            sb.append(MessageFormat.format("A user named {0} ",userRecord.name ));
            sb.append(MessageFormat.format("gender {0}, email {1} has ", userRecord.gender, userRecord.email));
        }

        sb.append(MessageFormat.format("made total purchase of {0}", amount));
        return sb.toString();
    }
}
