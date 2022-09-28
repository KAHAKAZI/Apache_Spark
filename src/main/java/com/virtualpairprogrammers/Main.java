package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

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
        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

        /*
            reduce - has to return the same type as the input type

            the below syntax can be replaced with myRDD.reduce(Double::sum)
         */
//        Integer result = myRDD.reduce( (value1, value2) -> value1 + value2 );
//        System.out.println(result);

        /*
            mapping values
         */
//        JavaRDD<Double> sqrtRDD = originalIntegers.map(value -> Math.sqrt(value));
//        sqrtRDD.collect().forEach(System.out::println);

        /*
            count
         */
//        System.out.println("count: " + sqrtRDD.count());

        /*
            count using map and reduce
         */
//        Long countResult = myRDD.map(value -> 1L).reduce((value1, value2) -> value1 + value2);
//        System.out.println("map reduce count: " + countResult);

        /*
            Tuples by means of classic Java
         */
        JavaRDD<IntegerWithSquareRoot> sqrtRDD = originalIntegers.map(value -> new IntegerWithSquareRoot(value));
        sqrtRDD.foreach(v -> System.out.println(v));

        /*
            Scala Tuple2 type
         */
        JavaRDD<Tuple2<Integer, Double>> sqrtRDDTuple = originalIntegers.map(value -> new Tuple2(value, Math.sqrt(value)));
        sqrtRDDTuple.collect().forEach(System.out::println);

        sc.close();
    }
}
