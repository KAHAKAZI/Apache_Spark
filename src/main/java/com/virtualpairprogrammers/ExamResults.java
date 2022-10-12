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
        dataset.groupBy("subject").max("score").show();

        // 2) without inferSchema
        dataset = spark.read().option("header", "true").csv("src/main/resources/exams/students.csv");
        // original .max() method has casting problem of string to numeric
//        dataset.groupBy("subject").max("score").show();
        // max() inside of agg() does casting by default
        dataset.groupBy("subject").agg(max("score").as("max_score")).show();
        // .agg() allows casting but it's not mandatory to obtain correct results
        dataset.groupBy("subject").agg(max("score").cast(DataTypes.IntegerType).as("max_score")).show();

        dataset.groupBy("subject").agg(
                max("score").as("max_score"),
                min("score").as("min_score")).show();

    }

}
