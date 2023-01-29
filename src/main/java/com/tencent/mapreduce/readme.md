##程序说明
    程序利用mapreduce方式，计算某个hdfs目录下的所有文档的单词统计数量。
##执行方式
    hadoop jar TBDSDEMOV5300-1.0-SNAPSHOT.jar com.tencent.mapreduce.WordCountDriver /myhdfs/test.txt /myhdfs/out.txt
##参数说明
    参数1：/tmp/input 表示输入文件目录（这里是hdfs目录，不是linux文件系统的本地目录）
    参数2：/tmp/output/0001 表示输出文件目录（这里是hdfs目录，不是linux文件系统的本地目录）