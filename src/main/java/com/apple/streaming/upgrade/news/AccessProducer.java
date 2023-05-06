package com.apple.streaming.upgrade.news;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @Program: spark-java
 * @ClassName: AccessProducer
 * @Description: 访问日志kafka Producer
 * @Author Mr.Apple
 * @Create: 2021-09-02 10:25
 * @Version 1.1.0
 **/
public class AccessProducer extends Thread {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static Random random = new Random();
    private static String[] sections = new String[]
            {"country", "international", "sport", "entertainment", "movie", "carton", "tv-show", "technology", "internet", "car"};
    private static int[] arr = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    private static String date;
    String bootstrap_servers = "192.168.230.2:9092,192.168.230.3:9092,192.168.230.4:9092";
    private Producer<Integer, String> producer;
    private String topic;

    public AccessProducer(String topic) {
        this.topic = topic;
        producer = new KafkaProducer<Integer, String>(PropertiesConfig());
        date = sdf.format(new Date());
    }

    public static void main(String[] args) {
        AccessProducer producer = new AccessProducer("news-access");
        producer.start();
    }

    private Properties PropertiesConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap_servers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Override
    public void run() {
        super.run();
        int counter = 0;
        while (true) {
            for (int i = 0; i < 100; i++) {
                String log = null;
                if (arr[random.nextInt(10)] == 1) {
                    log = getRegisterLog();
                } else {
                    log = getAccessLog();
                }
                ProducerRecord message = new ProducerRecord<String, String>(topic, log);
                producer.send(message);
                counter++;
                if (counter == 100) {
                    counter = 0;
                    try {
                        Thread.sleep(1000);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private String getAccessLog() {
        System.out.println("=====getAccessLog=====");
        StringBuffer buffer = new StringBuffer("");

        // 生成时间戳
        long timestamp = System.currentTimeMillis();

        // 生成随机userid（默认1000注册用户，每天1/10的访客是未注册用户）
        Long userid = 0L;

        int newOldUser = arr[random.nextInt(10)];
        if (newOldUser == 1) {
            userid = null;
        } else {
            userid = (long) random.nextInt(1000);
        }

        // 生成随机pageid（总共1k个页面）
        Long pageid = (long) random.nextInt(1000);

        // 生成随机版块（总共10个版块）
        String section = sections[random.nextInt(10)];

        // 生成固定的行为，view
        String action = "view";

        return buffer.append(date).append(" ")
                .append(timestamp).append(" ")
                .append(userid).append(" ")
                .append(pageid).append(" ")
                .append(section).append(" ")
                .append(action).toString();
    }

    private String getRegisterLog() {
        System.out.println("=====getRegisterLog=====");
        StringBuffer buffer = new StringBuffer("");

        // 生成时间戳
        long timestamp = System.currentTimeMillis();

        // 新用户都是userid为null
        Long userid = null;

        // 生成随机pageid，都是null
        Long pageid = null;

        // 生成随机版块，都是null
        String section = null;

        // 生成固定的行为，view
        String action = "register";

        return buffer.append(date).append(" ")
                .append(timestamp).append(" ")
                .append(userid).append(" ")
                .append(pageid).append(" ")
                .append(section).append(" ")
                .append(action).toString();
    }
}
