package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49937);
        inputData.add(90.32);
        inputData.add(20.32);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        /*
            .setMaster("local[*]") is a performance remark which means "use all available cores on the machine"
            .setMaster("local") would mean "run on a single thread"
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        /*
            JavaSparkContext represents a connection to a Spark cluster
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
            loading data and turning it into RDD - in reality not loaded but added to the execution plan
            the data would be loaded when performing some operations on it
         */
        JavaRDD<Double> myRDD = sc.parallelize(inputData);


        /*
            reduce - has to return the same type as the input type

            the below syntax can be replaced with myRDD.reduce(Double::sum)
         */
        Double result = myRDD.reduce( (value1, value2) -> value1 + value2 );

        System.out.println(result);

        sc.close();
    }
}
