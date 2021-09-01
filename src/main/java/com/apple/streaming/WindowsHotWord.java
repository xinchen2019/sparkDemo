package com.apple.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: WindowsHotWord
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-24 09:38
 * @Version 1.1.0
 **/
public class WindowsHotWord {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("WindowsHotWord");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        //日志级别
        jssc.sparkContext().setLogLevel("ERROR");

        JavaReceiverInputDStream<String> searchLogDStream = jssc.socketTextStream("master", 9999);
        JavaDStream<String> searchWordsDStream = searchLogDStream.map(
                new Function<String, String>() {
                    @Override
                    public String call(String searchLog) throws Exception {
                        return searchLog.split(" ")[1];
                    }
                });
        JavaPairDStream<String, Integer> searchWordPairDStream = searchWordsDStream.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String searchWord) throws Exception {
                        return new Tuple2<String, Integer>(searchWord, 1);
                    }
                });

        JavaPairDStream<String, Integer> searchWordCountsDStream = searchWordPairDStream
                .reduceByKeyAndWindow(
                        new Function2<Integer, Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                return v1 + v2;
                            }
                        }, Durations.seconds(60), Durations.seconds(10));


        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaPairRDD<String, Integer> call(
                            JavaPairRDD<String, Integer> searchWordCountsRDD) throws Exception {

                        JavaPairRDD<Integer, String> countSearchWordsRDD = searchWordCountsRDD
                                .mapToPair(
                                        new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                                            private static final long serialVersionUID = 1L;

                                            @Override
                                            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                                                return new Tuple2<Integer, String>(tuple._2, tuple._1);
                                            }
                                        });

                        JavaPairRDD<Integer, String> sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false);

                        JavaPairRDD<String, Integer> sortedSearchWordCountsRDD = sortedCountSearchWordsRDD
                                .mapToPair(
                                        new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                                            private static final long serialVersionUID = 1L;

                                            @Override
                                            public Tuple2<String, Integer> call(
                                                    Tuple2<Integer, String> tuple)
                                                    throws Exception {
                                                return new Tuple2<String, Integer>(tuple._2, tuple._1);
                                            }
                                        });
                        List<Tuple2<String, Integer>> hogSearchWordCounts = sortedSearchWordCountsRDD.take(3);

                        return searchWordCountsRDD;
                    }
                });

        finalDStream.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
