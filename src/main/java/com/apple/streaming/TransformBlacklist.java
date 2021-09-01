package com.apple.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: TransformBlacklist
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-23 16:00
 * @Version 1.1.0
 **/

/***
 * nc -lk 9999
 * 样例数据
 * 20151001 jerry
 * 20151001 kitty
 * 20151001 tom
 */
public class TransformBlacklist {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("TransformBlacklist");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
        //黑名单数据
        List<Tuple2<String, Boolean>> blacklistData = new ArrayList<Tuple2<String, Boolean>>();
        blacklistData.add(new Tuple2<>("tom", true));


        //黑名单RDD
        final JavaPairRDD<String, Boolean> blacklistRDD =
                jssc.sparkContext().parallelizePairs(blacklistData);

        JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("master", 9999);

        JavaPairDStream<String, String> userAdsClickLogDstream = adsClickLogDStream.mapToPair(
                new PairFunction<String, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(String adsClickLog)
                            throws Exception {
                        System.out.println(adsClickLog.split(" ")[1] + "原数据：" + adsClickLog);
                        return new Tuple2<>(adsClickLog.split(" ")[1], adsClickLog);
                    }
                });

        JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDstream.transform(
                new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD)
                            throws Exception {
                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD =
                                userAdsClickLogRDD.leftOuterJoin(blacklistRDD);
                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinedRDD.filter(
                                new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple)
                                            throws Exception {
                                        if (tuple._2._2().isPresent() && tuple._2._2.get()) {
                                            return false;
                                        }
                                        return true;
                                    }
                                });
                        JavaRDD<String> validAdsClickLogRDD = filterRDD.map(
                                new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple)
                                            throws Exception {
                                        return tuple._2._1;
                                    }
                                });
                        return validAdsClickLogRDD;
                    }
                });
        validAdsClickLogDStream.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
        jssc.close();

    }
}
