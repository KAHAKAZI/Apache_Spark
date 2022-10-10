package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MainSparkSQL {

    public static void main(String[] args) {

//        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

//        spark.read().csv("src/resources/exams/students.csv");

        /*
            add ' .option("header", "true") ' if the header is present in the datafile
         */
        Dataset<Row> dataset = spark.read().option("header", "true").csv("src/main/resources/exams/students.csv");

        /*
            print first 20 rows
         */
//        System.out.println("Print first 20 rows");
//        dataset.show();

//        long numberOfRows = dataset.count();
//        System.out.println("NoOfRows: " + numberOfRows);


        //--------------------------------------------------------------------------------------------
        // Filtering
        Row firstRow = dataset.first();

        /*
            from .csv all values are expected to be of type String
            get value by column index
         */
        System.out.println("\nPrint value from first row from column at index 2 - subject");
        String subject = (String) firstRow.get(2);
        System.out.println(subject);

        // get value by column name
        System.out.println("\nPrint value based on column name");
        subject = firstRow.getAs("subject").toString();
        System.out.println(subject);

        String year = (String) firstRow.getAs("year");
//        String year = firstRow.getAs("year").toString();
        System.out.println(year);

        System.out.println("\nPrint year as integer");
        int yearInt = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(yearInt);

        /*
            Filtering - expressions
         */
//        Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art'");
//        Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007");

        /*
            Filtering - lambdas
            ?
            Task not serializable
            Caused by: java.io.NotSerializableException: com.virtualpairprogrammers.MainSparkSQL
         */
//        Dataset<Row> modernArtResults = dataset.filter((Function1<Row, Object>) row ->
//                row.getAs("subject").equals("Modern Art") &&
//                        Integer.parseInt(row.getAs("year")) >= 2007);
//        modernArtResults.collectAsList().forEach(System.out::println);

        /*
            Filtering - columns
         */
        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");
        /*
            usage of static method from 'functions' class
         */
        Column scoreColumn = functions.column("score");
//        Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
//                .and(yearColumn.geq("2007")
//                .and(scoreColumn.geq("70"))));
//        modernArtResults.show();


        //--------------------------------------------------------------------------------------------
        // Full SQL syntax
        /*
            create in-memory view
         */
//        dataset.createOrReplaceTempView("my_students_view");
//        Dataset<Row> frenchResults = spark.sql("select * from my_students_view where subject='French'");
//        frenchResults.show();

        //--------------------------------------------------------------------------------------------
        // In-memory data
        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset1 = spark.createDataFrame(inMemory, schema);
//        dataset1.show();

        dataset1.createOrReplaceTempView("logging_table");
//        spark.sql("select level, count(datetime) from logging_table group by level").show();
//        spark.sql("select level, collect_list(datetime) from logging_table group by level").show();


        /*
            Date time format
         */
//        spark.sql("select level, month(datetime) from logging_table").show();
//        spark.sql("select level, date_format(datetime, 'M') as month from logging_table").show();
//        spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table").show();
        Dataset<Row> levelAndMonth = spark.sql("select level, date_format(datetime, 'MMMM') as month from logging_table");
        Dataset<Row> levelAndMonthAndCount = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) from logging_table group by level, month");
        levelAndMonthAndCount.show();


        spark.close();
    }
}
