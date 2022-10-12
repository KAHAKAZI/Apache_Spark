package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class ExamResults {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        // 1)
        // .option("inferSchema", true) - infer the schema from the dataset to allow .max() method
        Dataset<Row> dataset = spark.read().option("header", "true").option("inferSchema", true).csv("src/main/resources/exams/students.csv");
//        dataset.groupBy("subject").max("score").show();

        // 2) without inferSchema
        dataset = spark.read().option("header", "true").csv("src/main/resources/exams/students.csv");
        // original .max() method has casting problem of string to numeric
//        dataset.groupBy("subject").max("score").show();
        // max() inside of agg() does casting by default
//        dataset.groupBy("subject").agg(max("score").as("max_score")).show();
        // .agg() allows casting but it's not mandatory to obtain correct results
//        dataset.groupBy("subject").agg(max("score").cast(DataTypes.IntegerType).as("max_score")).show();

//        dataset.groupBy("subject").agg(
//                max("score").as("max_score"),
//                min("score").as("min_score")).show();

        aggregationTask(dataset);

    }

    /**
     * Requirements:
     * - build a pivot table, showing each subject down the 'left hand side'
     * - years across the top
     * - for each subject and year:
     *  - the average exam score
     *  - the standard deviation of scores ( use stddev() )
     *  - all results up to 2 decimal places ( use round() )
     */
    private static void aggregationTask(Dataset<Row> dataset) {
        dataset.groupBy("subject").pivot("year").agg(
                round(avg("score"), 2).as("avg"),
                round(stddev("score"), 2).as("stddev")).show();
    }

}
