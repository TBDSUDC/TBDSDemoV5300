###执行方式
####建表案例TableDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.TableDemo mytest:user cf_info
```
####添加数据案例PutDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.PutDemo mytest:user20230207
```
####查询数据案例ScanDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.ScanDemo mytest:user20230207 cf_info name
```
####获取数据GetDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.GetDemo 54321000000000000000092233703608605719362 cf_info name mytest:user20230207 
```
####删除数据DeleteDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.DeleteDemo 543210000000000000000922337036102993838 mytest:user20230207 cf_info cf_info:name 
```
####合并分区AdminDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.AdminDemo mytest:user20230207
```
####查询数据FirstKeyOnlyFilterDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.query.FirstKeyOnlyFilterDemo mytest:user20230207
```
####查询数据KeyOnlyFilterDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.query.KeyOnlyFilterDemo mytest:user20230207 
```
####查询数据PrefixFilterDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.query.PrefixFilterDemo mytest:user20230207 12345 
```
####查询数据SingleColumnValueFilterDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.query.SingleColumnValueFilterDemo mytest:user20230207 cf_info name age 
```
####查询数据TimestampsFilterDemo
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.query.TimestampsFilterDemo mytest:user20230207 1675766375346 
```
####加载协处理器
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.observer.LoadCoprocessor hdfs://tbds-11-199-128-237:8020/myhdfs/TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar mytest:user20230207 cf_info 
```
####卸载协处理器
```shell
hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT-jar-with-dependencies.jar com.tencent.hbase.observer.UnLoadCoprocessor mytest:user20230207 cf_info
```