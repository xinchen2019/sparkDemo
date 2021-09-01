package com.apple.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: Union
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-28 18:26
 * @Version 1.1.0
 **/
public class Union {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Union")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String[] arr1 = {"长江大桥1", "长江大桥2", "长江大桥3"};
        Integer[] arr2 = {4, 5, 6};

        JavaRDD arr1RDD = sc.parallelize(Arrays.asList(arr1));
        JavaRDD arr2RDD = sc.parallelize(Arrays.asList(arr2));

        List list = arr1RDD.union(arr2RDD).collect();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(": " + list.get(i));
        }
    }
}
