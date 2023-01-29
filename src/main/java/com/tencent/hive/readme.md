###功能说明
> 本demo演示了用jdbc方式操作hive ，包括删除表，创建表，insert数据，load数据，查询数据，show table等操作，avro、csv、json、orc、parquet等数据格式表的创建。
###类说明
> com.tencent.hive.HiveAvroTest 类演示了hive 表avro格式的文件数据操作
> 
> com.tencent.hive.HiveCsvTest 类演示了hive 表csv格式的文件数据操作
> 
> com.tencent.hive.HiveJsonTest 类演示了hive 表avro格式的文件数据操作
> 
> com.tencent.hive.HiveOrcAndSnappyTest 类演示了hive 表orc+snappy压缩的文件数据操作
>
> com.tencent.hive.HiveOrcTest 类演示了hive 表orc格式的文件数据操作
>
> com.tencent.hive.HiveParquetTest 类演示了hive 表parquet格式的文件数据操作

> com.tencent.hive.HiveService 类演示了hive表jdbc的基本操作

####加载本地文件到数据表
load data local inpath '/tmp/testlog/data.txt' into table users
####加载hdfs文件到数据表
load data  inpath '/tmp/testlog/data.txt' into table users

###执行方法说明
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hive.HiveCsvTest jdbc:hive2://11.199.128.xxx:10001/myhivetest;transportMode=http;httpPath=cliservice xxxx  xxxx
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hive.HiveEngineSelectTest jdbc:hive2://11.199.128.xxx:10001/myhivetest;transportMode=http;httpPath=cliservice xxxx  xxxx
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hive.HiveJsonTest jdbc:hive2://11.199.128.xxx:10001/myhivetest;transportMode=http;httpPath=cliservice xxxx  xxxx
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hive.HiveOrcAndSnappyTest jdbc:hive2://11.199.128.xxx:10001/myhivetest;transportMode=http;httpPath=cliservice xxxx  xxxx
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hive.HiveParquetTest jdbc:hive2://11.199.128.xxx:10001/myhivetest;transportMode=http;httpPath=cliservice xxxx  xxxx
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hive.HiveAvroTest jdbc:hive2://11.199.128.xxx:10001/myhivetest;transportMode=http;httpPath=cliservice xxxx  xxxx
```
