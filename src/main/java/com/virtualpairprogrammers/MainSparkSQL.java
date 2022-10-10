package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

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
        Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
                .and(yearColumn.geq("2007")
                .and(scoreColumn.geq("70"))));
        modernArtResults.show();


        //--------------------------------------------------------------------------------------------


        spark.close();
    }
}
