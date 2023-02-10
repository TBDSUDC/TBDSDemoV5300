package com.tencent.hbase.query;

import java.io.IOException;

import com.tencent.hbase.BaseDemo;
import com.tencent.hbase.HBaseConnectionFactory;
import com.tencent.hbase.RowKeyUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * PrefixFilter过滤器用来匹配行键，包含指定前缀的数据。
 * 这个过滤器所实现的功能其实也可以由RowFilter结合RegexComparator或者Scan指定开始行键和结束行键来实现，
 * 下面使用PrefixFilter实现查询ID为12345的用户的所有用户行为数据.
 */
public class PrefixFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("查询表名及条件不能为空");
            System.exit(0);
        }
        String tableName = args[0];
        Integer colVal = Integer.valueOf(args[1]);
        ScanWithPrefixFilter(tableName, colVal);
    }

    private static void ScanWithPrefixFilter(String tableName, Integer colVal) throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            RowKeyUtil rowKeyUtil = new RowKeyUtil();
            /**
             * 查询ID为12345的用户的所有用户行为数据
             */
            PrefixFilter prefixFilter = new PrefixFilter(
                    Bytes.toBytes(rowKeyUtil.formatUserId(colVal)));
            scan.setFilter(prefixFilter);
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
