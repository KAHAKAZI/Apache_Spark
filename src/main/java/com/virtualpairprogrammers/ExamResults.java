package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class ExamResults {

    /**
     * Old-fashioned java syntax of an anonymous in a class,
     * can be used in java 1-7 if the lambda can not be used
     */
    private static UDF2<String, String, Boolean> hasPassedFunction = new UDF2<String, String, Boolean>() {
        @Override
        public Boolean call(String grade, String subject) throws Exception {
            if (subject.equals("Biology")) {
                if (grade.startsWith("A"))
                    return true;
                return false;
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }
    };

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

//        aggregationTask(dataset);

        // UserDefinedFunctions
//        udf(dataset);

        System.out.println("\nGrade A pass:");
        /*
            Registering the UDF named 'hasPassed'
         */
        spark.udf().register(
                "hasPassed", // name
                (String grade) -> // input
                        grade.equals("A+"), // function
                DataTypes.BooleanType // return type
        );
        /*
            Using the previously defined UDF
         */
        dataset.withColumn("pass", callUDF("hasPassed", col("grade"))).show();

        System.out.println("\nGrade A | B | C pass:");
        spark.udf().register("hasPassed2", (String grade) -> {
//            return grade.matches("\"A+\"|A|B|C"); // does not take 'A+' into account ??
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
                },
                DataTypes.BooleanType
        );
        dataset.withColumn("pass", callUDF("hasPassed2", col("grade"))).show();

        System.out.println("\nBiology pass:");
        spark.udf().register("hasPassed3", (String grade, String subject) -> {
            if (subject.equals("Biology")) {
                if (grade.startsWith("A"))
                    return true;
                return false;
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
                },
                DataTypes.BooleanType
        );
        dataset.withColumn("pass", callUDF("hasPassed3", col("grade"), col("subject")))
                .filter(col("subject").equalTo("Biology"))
                .show();


        /*
            Using anonymous class as UDF instead of lambda - in case of java 1-7
         */
        System.out.println("\nBiology pass java 1-7 without lambda:");
        spark.udf().register("hasPassedFunction", hasPassedFunction, DataTypes.BooleanType);
        dataset.withColumn("pass", callUDF("hasPassedFunction", col("grade"), col("subject")))
                .filter(col("subject").equalTo("Biology"))
                .show();

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

    private static void udf(Dataset<Row> dataset) {
        // add new column 'pass' and fill the rows with 'YES'
//        dataset.withColumn("pass", lit("YES")).show();

        dataset.withColumn("pass", lit(col("grade").equalTo("A+"))).show();
    }

}
