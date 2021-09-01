package com.apple.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Program: spark-java
 * @ClassName: LocalFile
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-07 15:51
 * @Version 1.1.0
 **/
public class LocalFile {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("LocalFile")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.textFile()


    }
}
