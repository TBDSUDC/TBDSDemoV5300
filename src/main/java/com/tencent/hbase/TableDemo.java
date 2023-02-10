package com.tencent.hbase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

/**
 * hbase 客户端API操作表
 */
public class TableDemo {
    /**
     * 创建表
     *
     * @param tableName   表名
     * @param familyNames 列族名
     */
    public boolean createTable(String tableName, String... familyNames) throws
            Exception {
        Admin admin = null;
        try {
            admin = HBaseConnectionFactory.getConnection().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            //TableDescriptorBuilder类描述一个表，ColumnFamilyDescriptor类描述一个列族
            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName));

            List<ColumnFamilyDescriptor> familyList = new ArrayList<>();
            for (String familyName : familyNames) {
                ColumnFamilyDescriptor oneFamily = ColumnFamilyDescriptorBuilder
                        .newBuilder(familyName.getBytes()).build();
                familyList.add(oneFamily);
            }
            TableDescriptor tableDes = tableBuilder.setColumnFamilies(familyList).
                    build();
            admin.createTable(tableDes);
            return true;
        } finally {
            if (null != admin) {
                admin.close();
            }
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public boolean deleteTable(String tableName) throws Exception {
        Admin admin = null;
        try {
            admin = HBaseConnectionFactory.getConnection().getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            //删除之前要将表禁用
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
            return true;
        } finally {
            if (null != admin) {
                admin.close();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("hbase 表名或列簇名不能为空");
        }
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        TableDemo tableDemo = new TableDemo();

        String tableNameSuffix = format.format(calendar.getTime());
        String tableName = args[0] + tableNameSuffix;
        //创建表
        System.out.println("创建表结果：" + tableDemo.createTable(tableName, args.length >= 2 ? args[1] : "cf" + System.currentTimeMillis()));

        calendar.add(Calendar.MONTH, -1);
        String lastMonthTableNameSuffix = format.format(calendar.getTime());
        String lastMonthTableName = args[0] + lastMonthTableNameSuffix;
        //删除表，删除之前要将表禁用
        System.out.println("删除表结果：" + tableDemo.deleteTable(lastMonthTableName));
    }

}
