package com.apple.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: ParquetLoadData
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-11 20:15
 * @Version 1.1.0
 **/
public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("ParquetLoadData");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> usersDF = sqlContext.read().parquet("data/users.parquet");
        usersDF.registerTempTable("users");
        Dataset<Row> userNamesDF = sqlContext.sql("select name from users");
        List<String> userNames = userNamesDF.javaRDD().map(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        }).collect();


        for (String userName : userNames) {
            System.out.println(userName);
        }

    }
}
