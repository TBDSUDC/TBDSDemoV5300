/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.spark.java.streaming;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      consumer-group topic1,topic2
 */

public final class JavaDirectKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>\n" +
                         "  <brokers> is a list of one or more Kafka brokers\n" +
                         "  <groupId> is a consumer group name to consume from topics\n" +
                         "  <srctopics> is a list of one or more kafka topics to consume from\n\n" +
                         "  <destopics> is a list of one or more kafka topics to produce");
      System.exit(1);
    }

    String brokers = args[0];
    String groupId = args[1];
    String srctopics = args[2];
    String desttopics= args[3];

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
    // 构建kafka消费者
    Set<String> topicsSet = new HashSet<>(Arrays.asList(srctopics.split(",")));
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
    kafkaParams.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    kafkaParams.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");

    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
        jssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(ConsumerRecord::value);
    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
        .reduceByKey((i1, i2) -> i1 + i2);

    wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
      @Override
      public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
          @Override
          public void call(Iterator<Tuple2<String, Integer>> tuple2itr) throws Exception {
            KafkaProducer<String,String> producer = new KafkaProducer<String,String>(getProducerProperties(brokers));
            while (tuple2itr.hasNext()){
                Tuple2<String, Integer> datas = tuple2itr.next();
                producer.send(new ProducerRecord<String,String>(desttopics,datas._1()+","+datas._2()));
                producer.flush();
              }
            producer.close();
          }
        });
      }
    });
    jssc.start();
    jssc.awaitTermination();
  }
  public static Properties getProducerProperties(String brokerList) {

    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("acks", "1");
    props.put("retries", 3);
    props.put("batch.size", 16384);
    props.put("linger.ms", 60000);
    props.put("buffer.memory", 1638400);//测试的时候设置太大，Callback 返回的可能会不打印。
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    props.put("security.protocol","SASL_PLAINTEXT");
    props.put("sasl.mechanism","PLAIN");
    props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");
    return props;
  }

}