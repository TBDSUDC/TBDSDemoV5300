##程序说明
    程序利用mapreduce方式，计算某个hdfs目录下的所有文档的单词统计数量。
##执行方式
    hadoop jar hdfsmrtestauthparra-1.0-SNAPSHOT.jar com.hadooptest.hdfsmrtestauthparra.WordCount /tmp/input /tmp/output/0001 wPl9zgEpT4pJlxKbv2mHrpJEPt9q3q56yFnp testuser LluWnYeM2qtaxqEwzGXwFftvCvNJ1g4F
##参数说明
    参数1：/tmp/input 表示输入文件目录（这里是hdfs目录，不是linux文件系统的本地目录）
    参数2：/tmp/output/0001 表示输出文件目录（这里是hdfs目录，不是linux文件系统的本地目录）
    参数3：wPl9zgEpT4pJlxKbv2mHrpJEPt9q3q56yFnp 表示id
    参数4：testuser 表示用户名
    参数5：LluWnYeM2qtaxqEwzGXwFftvCvNJ1g4F 表示key