package com.tencent.hbase.query;
import java.io.IOException;
import com.tencent.hbase.BaseDemo;
import com.tencent.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * SingleColumnValueFilter过滤器类似于关系数据库的where条件语句，
 * 通过判断数据行指定的列限定符对应的值是否匹配指定的条件（如等于1001）来决定是否将该数据行返回
 */
public class SingleColumnValueFilterDemo extends BaseDemo {

    public static void main(String[] args) throws Exception {
        if(args.length<4){
            System.out.println("查询表名,列簇名，列名不能为空");
            System.exit(0);
        }
        String tableName = args[0];
        String cfName = args[0];
        String colName = args[0];
        String colVal = args[0];
        doScanWithSingleColumnValueFilter(tableName,cfName,colName,colVal);
    }

    private static void doScanWithSingleColumnValueFilter(String tableName,String cfName,String colName,String colVal) throws IOException {
        Scan scan = new Scan();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            /**
             *  查询表s_behavior中列限定符为o、值为1004的数据
             */
            SingleColumnValueFilter singleColumnValueFilter = new
                    SingleColumnValueFilter(
                    Bytes.toBytes(cfName), Bytes.toBytes(colName),
                    CompareOperator.EQUAL, Bytes.toBytes(colVal));
            /**
             * 如果数据行不包含列限定符o，则不返回该行
             * 如果设置为false，则返回不包含列限定符o的数据行
             */
            singleColumnValueFilter.setFilterIfMissing(true);

            scan.setFilter(singleColumnValueFilter);
            connection = HBaseConnectionFactory.getConnection();
            table = connection.getTable(TableName.valueOf(tableName));
            resultScanner = table.getScanner(scan);
            printResult(resultScanner);
            /**
             输出结果：
             rowkey=54321000000000000000092233703937313212872
             qualifier=o,value=1004
             qualifier=v,value=1004
             rowkey=54321000000000000000092233703937313240882
             qualifier=o,value=1004
             qualifier=v,value=1004
             */
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
