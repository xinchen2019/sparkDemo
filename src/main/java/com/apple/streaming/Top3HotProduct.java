package com.apple.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Program: spark-java
 * @ClassName: Top3HotProduct
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-24 23:05
 * @Version 1.1.0
 **/

/**
 * 样例数据
 * leo iphone HuaWei
 * leo iphone Apple
 * tom iphone XiaomiMIX4
 * tom iphone SanXing
 */
public class Top3HotProduct {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Top3HotProduct");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.sparkContext().setLogLevel("ERROR");
        JavaReceiverInputDStream<String> productClickLogsDstream = jssc.socketTextStream("master", 9999);

        JavaPairDStream<String, Integer> categoryProductPairsDstream = productClickLogsDstream.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Integer> call(String productClickLog)
                            throws Exception {
                        String[] productClickLogSplited = productClickLog.split(" ");

                        for(int i=0;i<productClickLogSplited.length;i++){
                            System.out.println(i+": "+productClickLogSplited[i]);
                        }
                        return new Tuple2<String, Integer>(
                                productClickLogSplited[2] + "_" + productClickLogSplited[1],
                                1
                        );
                    }
                });

        JavaPairDStream<String, Integer> categoryProductCountsDStream = categoryProductPairsDstream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
                , Durations.seconds(60),
                Durations.seconds(10)
        );

        categoryProductCountsDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(JavaPairRDD<String, Integer> categoryProductCountsRDD) throws Exception {
                JavaRDD<Row> categoryProductCountRowRDD = categoryProductCountsRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> categoryProductCount) throws Exception {
                        String categroty = categoryProductCount._1.split("_")[0];
                        String product = categoryProductCount._1.split("_")[1];
                        Integer count = categoryProductCount._2;
                        return RowFactory.create(categroty, product, count);
                    }
                });

                List<StructField> structFields = new ArrayList<StructField>();
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));

                HiveContext hiveContext = new HiveContext(categoryProductCountsRDD.context());
                StructType structType = DataTypes.createStructType(structFields);

                Dataset<Row> categoryProductCountDF = hiveContext.createDataFrame(
                        categoryProductCountRowRDD, structType);
                categoryProductCountDF.registerTempTable("product_click_log");
                Dataset<Row> top3ProductDF = hiveContext.sql(
                        "SELECT category,product,click_count "
                                + "FROM ("
                                + "SELECT "
                                + "category, "
                                + "product, "
                                + "click_count, "
                                + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC ) rank "
                                + "FROM product_click_log "
                                + ") tmp "
                                + "WHERE rank<=3 ");
                top3ProductDF.show();
            }
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
