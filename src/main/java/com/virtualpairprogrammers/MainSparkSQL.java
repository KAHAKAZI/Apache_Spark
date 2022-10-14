package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class MainSparkSQL {

    public static void main(String[] args) {

//        System.setProperty("hadoop.home.dir", "c:/hadoop");

        /*
            Logs of INFO log level:
                ...
                INFO util.Utils: Successfully started service 'SparkUI' on port 4040.
                ...
                INFO ui.SparkUI: Bound SparkUI to 0.0.0.0, and started at http://10.0.2.15:4040
         */
//        Logger.getLogger("org.apache").setLevel(Level.INFO);
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

        /*
            spark.sql.shuffle.partitions - Configures the number of partitions to use when shuffling data for joins or aggregations.
            https://spark.apache.org/docs/latest/sql-performance-tuning.html
            default values is : 200
            The default number of partitions to use when shuffling data for joins or aggregations.
            Note: For structured streaming, this configuration cannot be changed between query restarts from the same checkpoint location.
            https://spark.apache.org/docs/latest/configuration.html
         */
        spark.conf().set("spark.sql.shuffle.partitions", "12");

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
        Column scoreColumn = column("score");
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
//        levelAndMonthAndCount.show();

        /*
            Multiple grouping
         */
        levelAndMonth.createOrReplaceTempView("logging_table_level_and_month");
//        spark.sql("select level, month, count(month) as cnt from logging_table_level_and_month group by level, month").show();

        Dataset<Row> bigLog = spark.read().option("header", "true").csv("src/main/resources/extras/biglog.txt");
        bigLog.createOrReplaceTempView("big_log");
//        spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as cnt from big_log group by level, month").show();
//        System.out.println("\nTotal levels:");
//        spark.sql("select sum(level) from big_log").show();
//        System.out.println("\nTotal datetimes:");
//        spark.sql("select sum(datetime) from big_log").show();

        /*
            Ordering
            date_format() uses String type so to order it correctly use cast() to int
         */
//        System.out.println("\nOrdered by means of SQL:");
//        spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as cnt from big_log group by level, month order by cnt desc").show();
//        System.out.println("\nOrdered by means of Spark API:");
//        spark.sql("select level, date_format(datetime, 'MMMM') as month, cast(first(date_format(datetime, 'M')) as int) as month_num, count(1) as cnt from big_log group by level, month")
//                .orderBy(functions.col("month_num"))
//                .orderBy(functions.col("level"))
//                .drop("month_num") // drop 'month_num' column from the results
//                .show(100);

        /*
            Pure Java DataFrames / Datasets API
         */
//        System.out.println("\nDataFrames:");
//        bigLog.select("level", "date_format(datetime, 'M')").show(); // won't work
//        bigLog.selectExpr("level", "date_format(datetime, 'MMMM') as month").show();
        bigLog = bigLog.select(col("level"), date_format(col("datetime"), "MMMM").as("month"), date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
//        bigLog.show();

        // DataFrame grouping
//        System.out.println("\nDataFrames grouping:");
//        bigLog.groupBy(col("level"), col("month"), col("monthnum"))
//                .count()
//                .orderBy(col("monthnum"), col("level"))
//                .drop("monthnum")
//                .show();

        //--------------------------------------------------------------------------------------------
        /*
            Pivot tables
         */
//        System.out.println("\nPivot tables:");
        // grouping by level and month
//        bigLog.groupBy("level").pivot("month").count().show();
//        bigLog.groupBy("level").pivot("monthnum").count().show();

        List<Object> columns = new ArrayList<>();
        columns.add("March");
//        bigLog.groupBy("level").pivot("month", columns).count().show();

        Object[] months = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
//        bigLog.groupBy("level").pivot("month", Arrays.asList(months)).count().show();

        // when nullable column of 'Auguuuust'
        // using .na().fill(1) aka 'Not Available' and fill nulls with 0's
        Object[] months2 = {"January", "February", "March", "April", "May", "June", "July", "August", "Auguuuust", "September", "October", "November", "December"};
//        bigLog.groupBy("level").pivot("month", Arrays.asList(months2)).count().na().fill(0).show();


        //--------------------------------------------------------------------------------------------
        /*
            UDF - definition
         */
//        System.out.println("\nUDF - before:");
//        dataset1.show(); // dataset before UDF
        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");
        spark.udf().register("monthNum", (String month) -> {
            Date inputDate = input.parse(month);
            return Integer.parseInt(output.format(inputDate));
            },
                DataTypes.IntegerType
                );

        /*
            Usage of UDF inside the SQL
         */
//        System.out.println("\nUDF - after:");
//        spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
//                "from logging_table " +
//                "group by level, month " +
//                "order by monthNum(month), level")
//                .show();


        //--------------------------------------------------------------------------------------------
        /*
            SparkSQL Performance
         */
//        bigLog = bigLog.select(col("level"),
//                date_format(col("datetime"), "MMMM").as("month"),
//                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

        System.out.println("\nSpark SQL Performance");
        bigLog.groupBy("level", "month", "monthnum")
                .count()
                .as("total")
                .orderBy("monthnum")
                .drop("monthnum")
                .show(100);

        bigLog.explain();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        /*
            Access Spark Web UI:
            Visit http://localhost:4040/jobs/ - Spark jobs
         */




        spark.close();
    }
}
