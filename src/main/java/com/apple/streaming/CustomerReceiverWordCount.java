package com.apple.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


/**
 * @Program: spark-java
 * @ClassName: WordCount
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-17 10:28
 * @Version 1.1.0
 **/
public class CustomerReceiverWordCount {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("CustomerReceiverWordCount");

        System.setProperty("HADOOP_USER_NAME", "ubuntu");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.sparkContext().setLogLevel("ERROR");
        JavaReceiverInputDStream<String> lines =
                jssc.receiverStream(new JavaCustomReceiver("master", 9999));


        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairDStream pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
