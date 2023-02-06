package com.tencent.spark.java.sql.jdbc;

import com.tencent.conf.ConfigurationManager;
import jodd.util.HashCode;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;

public class SparkFromMysql2Mysql implements Serializable {
    static Logger logger = Logger.getLogger(SparkFromMysql2Mysql.class);
    private static final long serialVersionUID = 1L;
    /**
     * 使用spark-sql从db中读取数据, 处理后再回写到Kafka
     */
    public void loadDBtoDB(SparkSession sparkSession, String url, String srcTable,String destTable, String username, String password) {

        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", url)
                .option("dbtable", srcTable)
                .option("user", username)
                .option("password", password)
                .load();
        jdbcDF.show();
        JavaRDD<Row> jdbcRDD = jdbcDF.javaRDD();
        JavaRDD<Row> writeDBJavaRDD = jdbcRDD.map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                Random random = new Random();
                return RowFactory.create(HashCode.hash(random.nextInt(),random.nextInt()),row.get(1));
            }
        });
        //动态构造DataFrame的元数据
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,false));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> resultDF = sparkSession.createDataFrame(writeDBJavaRDD,structType);
        resultDF.show();
        resultDF.write()
                .format("jdbc")
                .mode(SaveMode.Append)
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", url)
                .option("dbtable", destTable)
                .option("user", username)
                .option("password", password)
                // JDBC批大小，默认 1000，灵活调整该值可以提高写入性能
                .option("batchsize", 10000)
                // 事务级别，默认为 READ_UNCOMMITTED，无事务要求可以填 NONE 以提高性能
                .option("isolationLevel", "NONE")
                .save();
    }

    public static void main(String[] args) {
        /*
         * 优先从配置文件中获取JDBC的信息
         * */
        String url = ConfigurationManager.getProperty("mysqlurl");
        String srcTable = ConfigurationManager.getProperty("mysqlsrctable");
        String destTable = ConfigurationManager.getProperty("mysqldesttable");
        String username = ConfigurationManager.getProperty("mysqlusername");
        String password = ConfigurationManager.getProperty("mysqlpassword");
        if(args.length >=5){
            url = args[0];
            srcTable=args[1];
            destTable=args[1];
            username=args[2];
            password=args[3];
        }
        if (url==null || srcTable == null || destTable==null || username==null || password==null) {
            logger.info("参数值不能为空");
        }
        SparkFromMysql2Mysql loadDBtoKafka = new SparkFromMysql2Mysql();
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkFromMysql2Mysql")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        System.out.println(" ---------------------- start SparkFromMysql2Mysql ------------------------");
        loadDBtoKafka.loadDBtoDB(sparkSession,url,srcTable,destTable,username,password);
        System.out.println(" ---------------------- finish SparkFromMysql2Mysql -----------------------");
        sparkSession.stop();
    }
}
