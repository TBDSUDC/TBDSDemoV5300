package com.tencent.hbase.observer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.tencent.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
/**
动态卸载协处理器
*/
public class UnLoadCoprocessor {


   public static void main(String[] args) throws IOException {
      if (args.length < 2) {
         System.out.println("协处理器作用表名、列簇名不能为空");
         System.exit(0);
      }
      String tbName = args[1];
      String columnFamily = args[2];
      doUnLoadCoprocessor(tbName,columnFamily);
   }
    private static void doUnLoadCoprocessor(String tbName,String columnFamily) throws IOException {
       Connection connection = null;
       Admin admin = null;
       try {
          connection = HBaseConnectionFactory.getConnection();
          admin = connection.getAdmin();

          TableName tableName = TableName.valueOf(tbName);
          admin.disableTable(tableName);

          TableDescriptorBuilder tableBuilder = TableDescriptorBuilder
     .newBuilder(tableName);

          List<ColumnFamilyDescriptor> familyList = new ArrayList<>();
          ColumnFamilyDescriptor oneFamily = ColumnFamilyDescriptorBuilder
     .newBuilder(columnFamily.getBytes()).build();
          familyList.add(oneFamily);
          tableBuilder.setColumnFamilies(familyList);
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
