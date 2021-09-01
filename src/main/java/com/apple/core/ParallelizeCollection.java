package com.apple.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: ParallelizeCollection
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-07 15:41
 * @Version 1.1.0
 **/
public class ParallelizeCollection {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });
        System.out.println("1到10的累加器和：" + sum);
        sc.close();
    }
}
