package com.tencent.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanDemo {

    private static final long userId = 12345;

    public final static String MIN_TIME = "0000000000000000000";
    public final static String MAX_TIME = "9999999999999999999";

    private static RowKeyUtil rowKeyUtil = new RowKeyUtil();


    public static void main(String[] args) throws IOException, InterruptedException, ParseException {
        if (args.length < 3) {
            System.out.println("查询表名,列簇名,列名不能为空");
            System.exit(0);
        }
        String tableName = args[0];
        String columnFamily = args[1];
        String colName = args[2];
        doScanTable(tableName,columnFamily,colName);
    }

    private static void doScanTable(String tableName, String cf, String colName) throws ParseException, IOException {
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            Scan scan = new Scan();
            //设置需要扫描的数据列族
            scan.addFamily(Bytes.toBytes(cf));
            //设置需要扫描的数据列
            scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName));
            //设置扫描开始行键，结果包含开始行
            scan.withStartRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) +
                    MIN_TIME));
            //设置扫描结束行键，结果不包含结束行
            scan.withStopRow(Bytes.toBytes(rowKeyUtil.formatUserId(userId) +
                    MAX_TIME));
            //设置事务隔离级别
            scan.setIsolationLevel(IsolationLevel.READ_COMMITTED);
            //设置每次RPC请求读取数据行
            scan.setCaching(100);
            //设置扫描的数据时间范围为2023年1月1日到现在
            String startS = "2023-01-01";
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = dateFormat.parse(startS);
            scan.setTimeRange(startDate.getTime(), System.currentTimeMillis());
            //设置获取两个版本的数据
            scan.readVersions(2);
            //设置是否缓存读取的数据块，如果数据会被多次读取，则应该设置为true，如果数据仅被读取一次，则应该设置为false
            scan.setCacheBlocks(false);

            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            resultScanner = table.getScanner(scan);

            printResult(resultScanner);
            resultScanner.close();
            table.close();
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

    protected static void printResult(ResultScanner resultScanner) throws
            IOException {
        Result result = null;
        while ((result = resultScanner.next()) != null) {
            if (result.getRow() == null) {
                continue;// key-values=NONE
            }
            Cell[] cells = result.rawCells();
            System.out.println("rowkey=" + Bytes.toString(result.getRow()));
            for (Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("qualifier=" + qualifier + ",value=" + value);
            }
        }
    }

}
