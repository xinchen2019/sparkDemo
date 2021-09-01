package com.apple.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: PersistWordcount
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-23 15:17
 * @Version 1.1.0
 **/
public class PersistWordcount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("PersistWordcount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //jssc.sparkContext().setLogLevel("ERROR");

        jssc.checkpoint("hdfs://master:9000/wordcount_checkpoint");
        System.setProperty("HADOOP_USER_NAME", "ubuntu");
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
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
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

        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
                wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        Connection conn = ConnectionPool.getConnection();
                        Tuple2<String, Integer> wordCount = null;
                        while (wordCounts.hasNext()) {
                            wordCount = wordCounts.next();
                            String sql = "insert into wordcount(word,count) "
                                    + "values('" + wordCount._1 + "'," + wordCount._2 + ")";
                            //System.out.println("wordCount._1: " + wordCount._1);
                            //System.out.println("wordCount._2: " + wordCount._2);
                            //System.out.println("conn：" + conn);
                            Statement stmt = conn.createStatement();
                            //System.out.println("sql: " + sql);
                            stmt.executeUpdate(sql);
                        }
                        ConnectionPool.returnConnection(conn);
                    }
                });
            }
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
