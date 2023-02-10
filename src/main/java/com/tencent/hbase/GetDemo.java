package com.tencent.hbase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
/**
 * get 操作获取一行数据
 */
public class GetDemo {
    public static void main(String[] args) throws IOException, InterruptedException,
            ParseException {
        if(args.length<4){
            System.out.println("rowkey值，表名,列名，列簇名不能为空");
        }
        String rowkey = args[0];
        String colFamily = args[1];
        String colName = args[2];
        String tableName = args[3];
        List<Get> gets = new ArrayList<Get>();
        Get oneGet = new Get(Bytes.toBytes(rowkey));
        //设置需要获取的数据列族
        oneGet.addFamily(Bytes.toBytes(colFamily));
        //设置需要获取的数据列
        oneGet.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName));
        /**
         ．        *  设置获取的数据时间范围为2023-01-01到现在
         ．        */
        String startS = "2023-01-01";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = dateFormat.parse(startS);
        oneGet.setTimeRange(startDate.getTime(), System.currentTimeMillis());

        //设置获取的数据版本号为1
        oneGet.readVersions(1);
        gets.add(oneGet);

        Table table = HBaseConnectionFactory.getConnection().getTable(TableName.
                valueOf(tableName));
        Result[] results = table.get(gets);
        for (Result result : results) {
            if (null != result.getRow()) {
                Cell[] cells = result.rawCells();
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                for (Cell cell : cells) {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("qualifier=" + qualifier + ",value=" + value);
                }
            }
        }
        table.close();
        /**
         * 输出结果如下所示：
         * rowkey=54321000000000000000092233703937313212871
         * qualifier=o,value=1002
         * qualifier=v,value=1002
         */
    }
}
