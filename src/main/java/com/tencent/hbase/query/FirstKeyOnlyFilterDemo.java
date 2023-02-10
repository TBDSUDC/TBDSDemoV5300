package com.tencent.hbase.query;

import java.io.IOException;

import com.tencent.hbase.BaseDemo;
import com.tencent.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

/**
 * FirstKeyOnlyFilter过滤器使得查询结果只返回每行的第一个单元值，
 * 它通常与KeyOnlyFilter一起使用来执行高效的行统计操作
 */
public class FirstKeyOnlyFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        if(args.length<1){
            System.out.println("查询表名不能为空");
            System.exit(0);
        }
        scanWithFirstKeyOnlyFilter(args[0]);
    }

    private static void scanWithFirstKeyOnlyFilter(String tableName) throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {

            /**
              * 查询表所有数据，每行只返回第一列，通常与KeyOnlyFilter一起使用
              */
            scan.setFilter(new FirstKeyOnlyFilter());
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
