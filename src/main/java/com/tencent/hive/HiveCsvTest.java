package com.tencent.hive;

import java.sql.*;

import com.tencent.conf.ConfigurationManager;
import org.apache.log4j.Logger;

/**
 * hive jdbc 操作csv格式的数据：包含删除表，创建表，insert数据，load数据，查询数据，show table等操作。
 */
public class HiveCsvTest {
    static Logger logger = Logger.getLogger(HiveCsvTest.class);
    public static void main(String[] args) {
        /*
        * 优先从配置文件中获取hive jdbc链接的信息
        * */
        String hiveUrl = ConfigurationManager.getProperty("hiveurl");
        String puser = ConfigurationManager.getProperty("hiveuser");
        String ppassd = ConfigurationManager.getProperty("hivepassd");
        /*
        * 如果用户指定，则使用代码中指定的数据
        * */
        if(args.length==3){
            hiveUrl = args[0];
            puser=args[1];
            ppassd=args[2];
        }
        Connection conn = HiveService.getConn(hiveUrl,puser,ppassd);
        Statement stmt = null;
        try {
            stmt = HiveService.getStmt(conn);
            stmt.execute("drop table  if exists users");
            //拥有HDFS文件读写权限的用户才可以进行此操作
            logger.debug("drop table is susscess");
            stmt.execute("create table users(user_id int, fname string,lname string )  row format delimited fields terminated by ','");
            //拥有HDFS文件读写权限的用户才可以进行此操作
            logger.debug("create table is susscess");
            stmt.execute("insert into users(user_id, fname,lname) values(222,'yang','yang2')");//拥有HDFS文件读写权限的用户才可以进行此操作
            logger.debug("insert is susscess");
            stmt.execute("load data  inpath '/myhdfs/data.txt' into table users");//拥有HDFS文件读写权限的用户才可以进行此操作
            logger.debug("load data is susscess");
            String sql = "select * from users";
            ResultSet res = null;
            res = stmt.executeQuery(sql);
            ResultSetMetaData meta = res.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.print(meta.getColumnName(i) + "\t");
            }
            System.out.println();
            while (res.next()) {
                System.out.print(res.getInt(1) + "\t\t");
                System.out.print(res.getString(2) + "\t\t");
                System.out.print(res.getString(3));
                System.out.println();
            }
            logger.debug("select data is susscess");
            sql = "show tables ";
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            logger.debug("show table is susscess");
            // describe table
            sql = "describe users";
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            HiveService.closeStmt(stmt);
            HiveService.closeConn(conn);
        }
    }
}
