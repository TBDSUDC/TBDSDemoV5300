package com.tencent.kafka;

import com.tencent.conf.ConfigurationManager;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Properties;

public class MyKafkaProducer {
    static Logger logger = Logger.getLogger(MyKafkaProducer.class);
    public static void main(String[] args) {
        try {
            /*
             * 优先从配置文件中获取kafka的信息
             * */
            String topic = ConfigurationManager.getProperty("topic");
            String brokerList = ConfigurationManager.getProperty("brokerList");
            int messageCount = Integer.valueOf(ConfigurationManager.getProperty("messageCount"));
            /*
             * 如果用户指定，则使用代码中指定的数据
             * */
            if(args.length >=3){
                topic = args[0];
                brokerList=args[1];
                messageCount = Integer.valueOf(args[2]);
            }
            if (topic == null || brokerList==null) {
                logger.info("topic 和 brokerList 参数值不能为空");
            }

            Properties props = getProducerProperties(brokerList);

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

            /**
             *通过for循环生产数据(测试最多生产100万条)
             */
            for (int messageNo = 1; messageNo <= messageCount && messageNo <= 1000000; messageNo++) {

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "no:"+messageNo, "message:"+messageNo);
                producer.send(record,new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (metadata != null) {
                            System.out.println("message sent to partition(" + metadata.partition() +"),"+"offset(" + metadata.offset() + ")");
                        } else {
                            e.printStackTrace();
                        }
                    }
                });
            }
            producer.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Properties getProducerProperties(String brokerList) {

        //create instance for properties to access producer configs
        Properties props = new Properties();
        /*
         *1.这里指定server的所有节点
         *2. product客户端支持动态broke节点扩展，metadata.max.age.ms是在一段时间后更新metadata。
         */
        props.put("bootstrap.servers", brokerList);
        /*
         * Set acknowledgements for producer requests.
         * acks=0：意思server不会返回任何确认信息，不保证server是否收到，因为没有返回retires重试机制不会起效。
         * acks=1：意思是partition leader已确认写record到日志中，但是不保证record是否被正确复制(建议设置1)。
         * acks=all：意思是leader将等待所有同步复制broker的ack信息后返回。
         */
        props.put("acks", "1");
        /*
         * 1.If the request fails, the producer can automatically retry,
         * 2.请设置大于0，这个重试机制与我们手动发起resend没有什么不同。
         */
        props.put("retries", 3);
        /*
         * 1.Specify buffer size in config
         * 2. 10.0后product完全支持批量发送给broker，不乱你指定不同parititon，product都是批量自动发送指定parition上。
         * 3. 当batch.size达到最大值就会触发dosend机制。
         */
        props.put("batch.size", 16384);
        /*
         * Reduce the no of requests less than 0;意思在指定batch.size数量没有达到情况下，在5s内也回推送数据
         */
        props.put("linger.ms", 60000);
        /*
         * 1. The buffer.memory controls the total amount of memory available to the producer for buffering.
         * 2. 生产者总内存被应用缓存，压缩，及其它运算。
         */
        props.put("buffer.memory", 1638400);//测试的时候设置太大，Callback 返回的可能会不打印。
        /*
         * 可以采用的压缩方式：gzip，snappy
         */
        //props.put("compression.type", gzip);
        /*
         * 1.请保持producer，consumer 序列化方式一样，如果序列化不一样，将报错。
         */
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol","SASL_PLAINTEXT");
        props.put("sasl.mechanism","PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");

        return props;
    }
}
