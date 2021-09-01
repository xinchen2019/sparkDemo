package com.apple.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Program: spark-java
 * @ClassName: LineCount
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-07 16:06
 * @Version 1.1.0
 **/
public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LocalFile")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD lines = sc.textFile("data/hello.txt");
        lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator call(String str) throws Exception {
                return Arrays.asList(str.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer int0, Integer int1) throws Exception {
                return int0 + int1;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 + ":" + tuple._2);
            }
        });
        sc.close();
    }
}
