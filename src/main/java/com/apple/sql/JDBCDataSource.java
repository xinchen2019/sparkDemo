package com.apple.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * @Program: spark-java
 * @ClassName: JDBCDataSource
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-13 00:08
 * @Version 1.1.0
 **/
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("JDBCDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Map<String, String> options = new HashMap<String, String>();
        /**
         *     mysqlProperties.put("fetchsize","10000")     //批次取数数量
         *     mysqlProperties.put("lowerBound","1")        //确定分区
         *     mysqlProperties.put("upperBound","7")           //确定分区
         *     mysqlProperties.put("numPartitions","6")        //分区数量
         *     mysqlProperties.put("partitionColumn","par")    //分区字段
         *
         */

        options.put("url", "jdbc:mysql://master:3306/test01?useSSL=false&createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=UTF-8");
        options.put("user", "ubuntu");
        options.put("password", "123456");
        options.put("dbtable", "student_infos");

        sqlContext.read().format("jdbc")
                .options(options).load().show();

    }
}
