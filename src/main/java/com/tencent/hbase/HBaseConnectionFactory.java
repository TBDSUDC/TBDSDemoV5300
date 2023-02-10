package com.tencent.hbase;
import com.tencent.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
/**
 10．    * @author pengxu
 11．    */
public class HBaseConnectionFactory {

    private static Connection connection = null;

    static {
        createConnection();
    }

    private static synchronized void createConnection() {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.client.pause", "100");
        configuration.set("hbase.client.write.buffer", "10485760");
        configuration.set("hbase.client.retries.number", "5");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.client.scanner.timeout.period", "100000");
        configuration.set("hbase.rpc.timeout", "40000");
        /**．          *  设置连接地址．          *  2.x.y 版本使用．          */
        configuration.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty("hbase.zookeeper.quorum"));
        /**．          * 设置连接地址．          *  3.0.0 版本开始使用．          */
        //configuration.set("hbase.masters", "master1:1234");
        configuration.set("zookeeper.znode.parent","/hbase-unsecure");
        configuration.set("hbase.coprocessor.user.enabled","true");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }

    public static Connection getConnection() {
        return connection;
    }
}