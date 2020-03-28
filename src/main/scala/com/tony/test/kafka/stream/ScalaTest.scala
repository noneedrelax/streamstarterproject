package com.tony.test.kafka.stream


import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.KStream

import scala.util.Properties

object ScalaTest {
    def main(args: Array[String]): Unit = {
      val bootStrapServers = "localhost:9092"
      val properties = new Properties()
      // stream config
      properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color-4")
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
      // consumer config
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
      // get k stream from topic
      val topic = "fav-color-input-2"
      val intermidateTopic = "fav-color-intermidate-2"
      val sb = new StreamsBuilder
      val kstream: KStream[String, String] = sb.stream(topic)
      // map topic and parse data
      // get the color, DQ check and filter color, split by comma, write to kafka broker as intermmidate.
      kstream.mapValues[String]((value: String) => value.toLowerCase).filter // get comma an filter out the one with wrong format
      ((key: String, tuple: String) => tuple.split(",").length == 2).map // get the key
      ((key: String, tuple: String) => new KeyValue[_, _](tuple.split(",")(0), tuple.split(",")(1))).filter // filter out the color
      ((key: Any, value: Any) => value == "red" || value == "green" || value == "blue").to // move to kafka for ktable convert
      intermidateTopic
  }
}
