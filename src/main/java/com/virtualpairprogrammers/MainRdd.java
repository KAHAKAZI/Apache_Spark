package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainRdd {

    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("WARN: Tuesday 4 September 0406");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        /*
            setting Hadoop path instead of system environment
         */
//        System.setProperty("hadoop.home.dir", "/home/christopher/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        /*
            .setMaster("local[*]") is a performance remark which means "use all available cores on the machine"
            .setMaster("local") would mean "run on a single thread"
         */
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        /*
            Conf for EMR
         */
        SparkConf conf = new SparkConf().setAppName("startingSpark");

        /*
            JavaSparkContext represents a connection to a Spark cluster
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
            loading data and turning it into RDD - in reality not loaded but added to the execution plan
            the data would be loaded when performing some operations on it
         */
        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

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
//        JavaRDD<IntegerWithSquareRoot> sqrtRDD = originalStrings.map(value -> new IntegerWithSquareRoot(value));
//        sqrtRDD.foreach(v -> System.out.println(v));

        /*
            Scala Tuple2 type
         */
//        JavaRDD<Tuple2<Integer, Double>> sqrtRDDTuple = originalStrings.map(value -> new Tuple2(value, Math.sqrt(value)));
//        sqrtRDDTuple.collect().forEach(System.out::println);

//        JavaPairRDD<String, String> pairRDD = originalLogMessages.mapToPair(rawValue -> {
//            String[] columns = rawValue.split(":");
//            String level = columns[0];
//            String date = columns[1];
//
//            return new Tuple2<>(level, date);
//        });

        /*
            groupByKey may lead to severe performance problems so use only when neccessary
            returns: PairRDD<String, Iterable<String>> which does not allow to count values
         */
//        pairRDD.groupByKey();
//        sc.parallelize(inputData).mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
//                .groupByKey()
//                .foreach( tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

        // ------------------------------------------------

//        JavaPairRDD<String, Long> pairRDDCounts = originalLogMessages.mapToPair(rawValue -> {
//            String[] columns = rawValue.split(":");
//            String level = columns[0];
////            Long count = columns[1];
//
//            return new Tuple2<>(level, 1L);
//        });

//        JavaPairRDD<String, Long> sumsRDD = pairRDDCounts.reduceByKey((value1, value2) -> value1 + value2);
//        sumsRDD.foreach(tuple -> System.out.println(tuple._1 + ", "+ tuple._2));

        /*
            simplification of steps
         */
//        sc.parallelize(inputData).mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
//                .reduceByKey((value1, value2) -> value1 + value2)
//                .foreach(tuple -> System.out.println(tuple._1 + ", "+ tuple._2));

        /*
            flatMap
         */
//        System.out.println("\nFlatMap:");
//        sc.parallelize(inputData)
//                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
////                .filter( word -> true);   // pass all
////                .filter( word -> false);    // pass none
//                .filter( word -> word.length() > 1)
//                .collect().forEach(System.out::println);

        /*
            Reading from a file
         */
        System.out.println("\nReading from a file - FlatMap:");
        JavaRDD<String> initialRDD = sc.textFile("s3n://custom-named-location/input.txt");  // reading from an Amazon S3
//        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
//        initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//                .filter( word -> word.length() > 1)
//                .collect().forEach(System.out::println);

        /*
            Task
            1. Load input.txt into an RDD
            2. Get rid of boring words
            3. Count remaining words
            4. Find the ten most frequently used
         */
        System.out.println("\nTask:");
        initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .filter(sentence -> sentence.trim().length() > 0)
                .filter(Util::isNotBoring)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .groupByKey()
                .mapToPair(tuple -> new Tuple2<>(tuple._2.iterator().next(), tuple._1))
                .map(tuple -> new CustomTuple(tuple._1, tuple._2))
                .sortBy(value -> value, false, 1)
                .top(10).forEach(System.out::println);

        /*
            Solution from the course
            Regex: [^a-zA-Z\\s] : replace anything that is not in that range: a-z, A-Z, space
         */
        System.out.println("\nTask solution:");
        JavaRDD<String> lettersOnlyRdd = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);

        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);

        JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(Util::isNotBoring);

        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));

        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long,String> switched = totals.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        JavaPairRDD<Long,String> sorted = switched.sortByKey(false);

        /*
            take() takes top values regardless of partitioning
         */
        List<Tuple2<Long, String>> results = sorted.take(10);
        results.forEach(System.out::println);

        /*
            Simple but wrong explanation:
            The sort result in this case is incorrect because the data is sorted in partitions, not as a whole collection

            Explanation:
            forEach() / foreach() takes in PARALLEL values from partitions and prints them in different order, multithreading comes here into an account
            foreach() / forEach() executes the lambda on each partition in parallel
         */
//        sorted.collect().forEach(System.out::println);

        /*
            Printing sorted values not by means of faulty foreach()
         */
//        Iterator it = sorted.collect().iterator();
//        while (it.hasNext())
//            System.out.println(it.next());
//        System.out.println("Partitions: "+ sorted.getNumPartitions());

        /*
            coalesce() :
            * is sufficient only for small data
            * it created new big RDD and allows further operations
            * the result may be OutOfMemory
         */
//        sorted.coalesce(1).collect().stream().limit(10).forEach(System.out::println);



        sc.close();
    }
}
