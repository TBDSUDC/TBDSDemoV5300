package com.tencent.hbase.query;

import java.io.IOException;

import com.tencent.hbase.BaseDemo;
import com.tencent.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * TimestampsFilter过滤器可以用来过滤某个时间戳的数据，可以与Get操作和Scan操作一起使用
 */
public class TimestampsFilterDemo extends BaseDemo {


    public static void main(String[] args) throws Exception {
        if(args.length<1){
            System.out.println("查询表名不能为空");
            System.exit(0);
        }
        String tableName = args[0];
        Long timstamp = Long.valueOf(args[1]);
        doScanWithTimestampFilter(tableName,timstamp);
    }

    private static void doScanWithTimestampFilter(String tableName,Long timstamp) throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {

            /**
            *  查询表s_behavior中HBase时间戳为xxxxxL的数据
            */
            List<Long> timeStampList = new ArrayList<Long>();
            timeStampList.add(timstamp);
            TimestampsFilter timestampsFilter = new TimestampsFilter(timeStampList);
            scan.setFilter(timestampsFilter);
            /**
             ．           也可以使用下面的命令替换
             ．           scan.setTimestamp(1643123455499L);
             ．           scan.setTimeRange(1643123455499L,1643123455500L);
             ．           */
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
        } finally {
            if (null != resultScanner) {
                resultScanner.close();
            }
            if (null != table) {
                table.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
    }
}
