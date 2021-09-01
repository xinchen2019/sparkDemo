package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @Program: spark-java
 * @ClassName: ManuallySpecifyOptions
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-10 21:48
 * @Version 1.1.0
 **/
public class ManuallySpecifyOptions {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("ManuallySpecifyOptions");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> peopleDF = sqlContext.read().format("json")
                .load("data/people.json");
        peopleDF.select("name").write().format("parquet")
                .save("result/peopleName_java");
    }
}
