package com.tencent.hbase.observer;

import java.util.ArrayList;
import java.util.List;

import com.tencent.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * 动态加载协处理器
 */
public class LoadCoprocessor {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("协处理器hdfs路径、表名、列簇名不能为空");
            System.exit(0);
        }
        String jarPath = args[0];
        String tbName = args[1];
        String columnFamily = args[2];
        doLoadCoprocessor(jarPath, tbName, columnFamily);
    }

    private static void doLoadCoprocessor(String jarPath, String tbName, String columnFamily) throws IOException {
        Connection connection = null;
        Admin admin = null;
        try {

            connection = HBaseConnectionFactory.getConnection();
            admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(tbName);
            admin.disableTable(tableName);

            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);

            List<ColumnFamilyDescriptor> familyList = new ArrayList<>();
            ColumnFamilyDescriptor oneFamily = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes()).build();
            familyList.add(oneFamily);
            tableBuilder.setColumnFamilies(familyList);

            CoprocessorDescriptor coprocessorDescriptor = CoprocessorDescriptorBuilder
                    .newBuilder(KeyWordFilterRegionObserver.class.getCanonicalName()).setJarPath
                            (jarPath).setPriority(1001).build();
            tableBuilder.setCoprocessor(coprocessorDescriptor);

            admin.modifyTable(tableBuilder.build());
            admin.enableTable(tableName);
            System.out.println("success");
        } finally {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }

        }
    }
}
