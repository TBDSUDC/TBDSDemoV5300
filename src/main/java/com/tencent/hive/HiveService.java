package com.tencent.hive;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveService {
    static Logger logger = Logger.getLogger(HiveService.class);
    // Hive的JDBC驱动类
    public static String dirverName = "org.apache.hive.jdbc.HiveDriver";
    /**
     * 创建连接
     *
     * @return
     * @throws SQLException
     */
    public static Connection getConn(String url,String user,String passd) {
        Connection conn = null;
        try {
            Class.forName(dirverName);
            conn = DriverManager.getConnection(url, user, passd);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    /**
     * 创建命令
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public static Statement getStmt(Connection conn) throws SQLException {
        logger.debug(conn);
        if (conn == null) {
            logger.debug("this conn is null");
        }
        return conn.createStatement();
    }
    /**
     * 关闭连接
     *
     * @param conn
     */
    public static void closeConn(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    /**
     * 关闭命令
     *
     * @param stmt
     */
    public static void closeStmt(Statement stmt) {
        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
