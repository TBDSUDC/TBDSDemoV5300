###功能说明
> 本demo演示了利用java语言发送kafka消息和接受kafka消息的demo
###类说明
> com.tencent.kafka.MyKafkaProducer 类演示了kafka生产消息的demo
>
> com.tencent.kafka.MyKafkaConsumer 类演示了kafka消费消息的demo
>

###执行方法说明
####配置文件中已经配置了kafka相关的连接信息
```shell
java -cp TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.kafka.MyKafkaProducer
java -cp TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.kafka.MyKafkaConsumer
```
####配置文件中未配置了kafka相关的连接信息
```shell
java -cp TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.kafka.MyKafkaProducer topic信息 brokerlist信息(如：tbds-11-199-128-xxxx:6669,tbds-11-199-129-xxxx:6669,) 10000(生产消息条数)
java -cp TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.kafka.MyKafkaConsumer topic信息 brokerlist信息(如：tbds-11-199-128-xxxx:6669,tbds-11-199-129-xxxx:6669,) test(groupid)
```