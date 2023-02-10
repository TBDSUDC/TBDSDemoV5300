package com.tencent.hbase.query;

import java.io.IOException;

import com.tencent.hbase.BaseDemo;
import com.tencent.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

/**
 * KeyOnlyFilter过滤器使得查询结果只返回行键，
 * 主要是重写了transformCell()方法（该方法主要用来更改返回单元格的值），将单元格的值全部替换为空
 */
public class KeyOnlyFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        if(args.length<1){
            System.out.println("查询表名不能为空");
            System.exit(0);
        }
        scanWithKeyOnlyFilter(args[0]);
    }

    private static void scanWithKeyOnlyFilter(String tableName) throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            /**
             * 查询表s_behavior中所有数据的行键
             */
            scan.setFilter(new KeyOnlyFilter());

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
