package com.apple.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Program: spark-java
 * @ClassName: Persist
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-09 10:54
 * @Version 1.1.0
 **/
public class Persist {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("")
                .cache();

    }

}
