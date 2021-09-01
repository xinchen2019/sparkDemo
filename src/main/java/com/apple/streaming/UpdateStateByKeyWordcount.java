package com.apple.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
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
import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: UpdateStateByKeyWordcount
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-23 15:17
 * @Version 1.1.0
 **/
public class UpdateStateByKeyWordcount {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("user", "ubuntu");
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("UpdateStateByKeyWordcount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.sparkContext().setLogLevel("ERROR");

        jssc.checkpoint("hdfs://master:9000/wordcount_checkpoint");
        System.setProperty("HADOOP_USER_NAME", "ubuntu");
        System.setProperty("user", "ubuntu");
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("master", 9999);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                    @Override
                    public Optional<Integer> call(List<Integer> values,
                                                  Optional<Integer> state) throws Exception {
                        Integer newValue = 0;
                        if (state.isPresent()) {
                            newValue = state.get();
                        }
                        for (Integer value : values) {
                            newValue += value;
                        }
                        return Optional.of(newValue);
                    }
                });
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
