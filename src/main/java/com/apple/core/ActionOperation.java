package com.apple.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: ActionOperation
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-09 09:40
 * @Version 1.1.0
 **/
public class ActionOperation {
    public static void main(String[] args) {
        collect();
    }

    private static void collect() {
        SparkConf conf = new SparkConf()
                .setAppName("collect")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        JavaRDD<Integer> doubleNumbers = numbers.map(
                new Function<Integer, Integer>() {
                    private static final long serialVersionUID = 1L;


                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }
                });
        List<Integer> doubleNumberList = doubleNumbers.collect();
        for (Integer num : doubleNumberList) {
            System.out.println(num);
        }
        sc.close();
    }
}
