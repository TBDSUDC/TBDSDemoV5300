package com.tencent.kafka;

import com.tencent.conf.ConfigurationManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyKafkaConsumer implements Runnable{
    static Logger logger = Logger.getLogger(MyKafkaConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public MyKafkaConsumer(String topic,String brokerList, String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol","SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM,"PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");

        consumer = new KafkaConsumer<String,String>(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        while (true){
            try{
                doWork();
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at partition "+record.partition()+",offset " + record.offset());
        }
    }

    public static void main(String[] args) {
        /*
         * 优先从配置文件中获取kafka的信息
         * */
        String topic = ConfigurationManager.getProperty("topic");
        String brokerList = ConfigurationManager.getProperty("brokerList");
        String group = ConfigurationManager.getProperty("group");
        /*
         * 如果用户指定，则使用代码中指定的数据
         * */
        if(args.length >=3){
            topic = args[0];
            brokerList=args[1];
            group = args[2];
        }
        if (topic == null || brokerList==null || group==null) {
            logger.info("topic , brokerList ,group 参数值不能为空");
        }
        MyKafkaConsumer kafkaConsumerDemo = new MyKafkaConsumer(topic, brokerList,group);
        Thread thread = new Thread(kafkaConsumerDemo);
        thread.start();
    }
}
