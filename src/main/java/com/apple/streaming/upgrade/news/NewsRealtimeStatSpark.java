package com.apple.streaming.upgrade.news;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * @Program: spark-java
 * @ClassName: NewsRealtimeStatSpark
 * @Description: 新闻网站关键指标实时统计spark应用程序
 * @Author Mr.Apple
 * @Create: 2021-09-02 12:52
 * @Version 1.1.0
 * 参考链接 http://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html
 **/
public class NewsRealtimeStatSpark {


    public static void main(String[] args) throws InterruptedException {

        String bootstrap_servers = "192.168.230.2:9092,192.168.230.3:9092,192.168.230.4:9092";

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("NewsRealtimeStatSpark");
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(10));
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", bootstrap_servers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "newsgroup");
        //kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("enable.auto.commit", false);
        Set<String> topics = new HashSet<String>();
        topics.add("news-access");
        JavaInputDStream<ConsumerRecord<String, String>> lines =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        //过滤出日志
        JavaDStream<ConsumerRecord<String, String>> accessDStream = lines.filter(
                new Function<ConsumerRecord<String, String>, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(ConsumerRecord<String, String> v1) throws Exception {
                        String log = v1.value();
                        String[] logSplited = log.split(" ");
                        String action = logSplited[5];
                        if ("view".equals(action)) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

        //统计第一个指标：每10秒内的各个页面的pv
        calculatePagePv(accessDStream);
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    private static void calculatePagePv(JavaDStream<ConsumerRecord<String, String>> accessDStream) {

        JavaPairDStream<Long, Long> pageidDStream = accessDStream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> call(ConsumerRecord<String, String> record) throws Exception {
                        String log = record.value();
                        String[] logSplited = log.split(" ");
                        Long pageid = Long.valueOf(logSplited[3]);
                        return new Tuple2<Long, Long>(pageid, 1L);
                    }
                });

        JavaPairDStream<Long, Long> pagePvDStream = pageidDStream.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });
        pagePvDStream.print();
    }
}
