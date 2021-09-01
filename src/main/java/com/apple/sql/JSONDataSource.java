package com.apple.sql;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @Program: spark-java
 * @ClassName: JSONDataSource
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-12 09:40
 * @Version 1.1.0
 **/
public class JSONDataSource {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("JSONDataSource");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset studentScoresDF = sqlContext.read().json("data/students.json");
        studentScoresDF.registerTempTable("student_scores");

        Dataset goodStudentScoresDF = sqlContext
                .sql("select name,score from student_scores where score>= 80 ");
        List<String> goodStudentNames = goodStudentScoresDF.toJavaRDD().map(
                new Function<Row, String>() {
                    @Override
                    public String call(Row row) throws Exception {
                        return row.getString(0);
                    }
                }).collect();

        List<String> studentInfoJSONS = new ArrayList<String>();
        studentInfoJSONS.add("{\"name\":\"Leo\",\"age\":18}");
        studentInfoJSONS.add("{\"name\":\"Marry\",\"age\":17}");
        studentInfoJSONS.add("{\"name\":\"Jack\",\"age\":19}");
        JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONS);
        Dataset studentInfosDF = sqlContext.read().json(studentInfoJSONsRDD);

        studentInfosDF.registerTempTable("student_infos");

        String sql = "select name,age from student_infos where name in (";
        for (int i = 0; i < goodStudentNames.size(); i++) {
            sql += "'" + goodStudentNames.get(i) + "'";
            if (i < goodStudentNames.size() - 1) {
                sql += ",";
            }
        }
        sql += ")";
        sqlContext.sql(sql).show();
        System.out.println("==============");
        System.out.println(sqlContext.sql(sql).schema());
        System.out.println("==============");

        Dataset goodStudentInfosDF = sqlContext.sql(sql);
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD = goodStudentScoresDF.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(
                        row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join(goodStudentInfosDF.toJavaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {


                return new Tuple2<String, Integer>(
                        row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }));


        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(
                new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
            }
        });


        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);


        Dataset goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType);
        String outPath = "result/good-students";
        deletePath(sqlContext, outPath);
        goodStudentsDF.write().format("json").save(outPath);

    }


    /***
     * @Description: 删除输出目录
     * @Author: Apple
     * @Date: 2021/8/12 20:38
     * @Param sqlContext:
     * @Param path:
     * @return: void
     */
    public static void deletePath(SQLContext sqlContext, String path) throws IOException {
        Path file_path = new Path(path);
        FileSystem file_system = file_path.getFileSystem(sqlContext.sparkContext().hadoopConfiguration());
        if (file_system.exists(file_path)) {
            file_system.delete(file_path, true);
        }
    }
}
