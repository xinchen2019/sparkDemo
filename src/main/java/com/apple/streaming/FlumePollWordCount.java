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
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Program: spark-java
 * @ClassName: FlumePushWordCount
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-31 15:35
 * @Version 1.1.0
 **/
public class FlumePollWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("FlumePollWordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
        JavaReceiverInputDStream<SparkFlumeEvent> lines = FlumeUtils.createPollingStream(jssc, "192.168.230.2", 8888);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator call(SparkFlumeEvent event) throws Exception {
                String line = new String(event.event().getBody().array());
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
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
