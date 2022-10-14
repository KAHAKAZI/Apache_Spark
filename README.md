# Spark for Java Developers course

[Udemy - Spark for Java Developers](https://www.udemy.com/course/apache-spark-for-java-developers)

### Documentation:

[RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
* Resilient Distributed Datasets (RDDs) -> RDD Operations
  * [Transformactions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)
  * [Actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)

[Spark SQL, Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html)

[Static ' functions ' class and available methods](https://spark.apache.org/docs/1.5.0/api/java/org/apache/spark/sql/functions.html)

[Class RelationalGroupedDataset - a set of methods for aggregations on a DataFrame, created by groupBy, cube or rollup (and also pivot)](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/RelationalGroupedDataset.html)

[Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

[Hive - LanguageManual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
* Hive is built on top of Apache Hadoop, used for querying data warehouses

[Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html)

[SparkSQL - Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

[Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)

-------------------------------------------------------------
### Requirements:
* JDK 8 ( no support for JDK 9 )
* Lambda syntax

### Modules:
* Spark RDD ( Resilient Distributed Dataset )
* Spark SQL
  * Data Frames API
  * data science
* Spark ML ( Machine Learning )
* Spark Streaming
  * streams
  * apache kafka

-------------------------------------------------------------
### Functions:
* reduce
* map
* count

### Tuples 
* a collection of values that are not planned to be modified
* allow to keep data together
* a data structure consisting of multiple parts. 
* Hence Tuples can be defined as a data structure that can hold multiple values and these values may/may not be related to each other 
* example: [Geeks, 123, &#*@]

#### Scala implementation of Tuple
* new Tuple2() up to new Tuple22()

### JavaPairRDD
* allows multiple instances of the same key, something what is forbidden in Java Map<>
* groupByKey() does not allow to count the aggregated values
* reduceByKey()

### FlatMap

### Sorts and coalesce
* take() takes top values regardless of partitioning
* foreach() / forEach() executes the lambda on each partition in parallel so it may produce wrong results in case of sorted data

### S11. Deploying to EMR ( Elastic MapReduce )
* Spark supports the Hadoop cluster manager
* Hadoop is a collection of tools, one of which is EMR, HDFS filesystem, and cluster manager Hadoop YARN
* EMR is basically Amazon's implementation of Hadoop in the Cloud
* [Amazon EC2 On-Demand Pricing](https://aws.amazon.com/ec2/pricing/on-demand/)
  * m6g.xlarge 
* To read file in Spark application using AWS create [Amazon S3](https://s3.console.aws.amazon.com/s3), create a bucket and upload the file
* To build an application to get it ready for deployment run`mvn package`
* Enable SSH connection: In EMR -> Security and access -> Security groups for Master -> Master group -> Inbounds rules -> Edit -> Add rule
  * Port 18080 MyIP
  * Port 22 MyIP
* SSH to the instance:
```
  chmod 700 <your_key.pem>
  ssh -i <your_key.pem> hadoop@<Master_public_DNS>
```
ended at S11.28

## Spark SQL

* SparkSQL gives rich API to work with structured big data
* Dataset is immutable like RDD
* Dataframe is a Dataset of rows.
* DataFrames vs. Datasets: DataFrames relate to Datasets of rows but Datasets relate to datasets of anything

#### Pivot table vs. Flat table
* Pivot table is helpful when grouping on two separate columns, it gives more compact way of displaying results
* Probably it's impossible to do pivot table directly from sql syntax

#### Aggregations

#### UserDefinedFunctions
* udf allows to add new columns which are calculated based on other columns
* withColumn() allows to add a new column

#### SparkSQL performance
* underneath of Dataset / Dataframes are still RDDs
* Dataset still can offer RDDs datatype by means of:
  * dataset.rdd();
  * dataset.javaRDD();
  ##### Spark Web UI : http://localhost:4040/jobs/
* Spark SQL shuffle configuration
  * The Spark SQL shuffle is a mechanism for redistributing or re-partitioning data so that the data is grouped differently across partitions, based on your data size you may need to reduce or increase the number of partitions of RDD/DataFrame using spark.sql.shuffle.partitions configuration or through code
  * Spark shuffle is a very expensive operation as it moves the data between executors or even between worker nodes in a cluster so try to avoid it when possible. When you have a performance issue on Spark jobs, you should look at the Spark transformations that involve shuffling.
  * [Spark SQL Shuffle Partitions](https://sparkbyexamples.com/spark/spark-shuffle-partitions/)
* HashAggregation - more can be found about 'hashaggregate vs groupaggregate' in PostgreSQL documentation
* Grouping strategies
  * SparkSQL can use two algorithms for grouping
    * SortAggregate - will sort the rows and then gather together the matching rows - O(n*log n) but memory efficient
    * HashAggregate - O(n)
      * faster than SortAggregate
      * avoids the need for sorts at the cost of more memory
      * SparkSQL will use HashAggregate where possible
      * HashAggregation is only possible IF the data for the 'value' is mutable
      * Spark HashMap / HashTable uses native memory in the opposition to HashMap from Java API, it's not stored in the regular Java Heap. It uses memory directly. No GC etc.
      * An Unsafe implementation of Row which is backed by raw memory instead of Java objects - [UnsafeRow.java](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeRow.java) - removed since Java 9
      * Overwritable / Mutable data types from the [UnsafeRow.java](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeRow.java) class :
```aidl
// DecimalType, DayTimeIntervalType and YearMonthIntervalType are also mutable
  static {
    mutableFieldTypes = Collections.unmodifiableSet(
      new HashSet<>(
        Arrays.asList(
          NullType,
          BooleanType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          FloatType,
          DoubleType,
          DateType,
          TimestampType,
          TimestampNTZType
        )));
  }
```
* String in comparison to ex. Integer is immutable because it's size in the memory is not bounded
* Spark SQL vs. Java API ( DataFrames ) performance:
  * main difference between the two may comes from the ' HashAggregation vs GroupAggregation'


-------------------------------------------------------------
### Abbreviations:
* RDD Resilient Distributed Dataset
* HDFS Hadoop Distributed File System
  * HDFS provides better data throughput than traditional file systems, in addition to high fault tolerance and native support of large datasets
* YARN Yet Another Resource Negotiator – Manages and monitors cluster nodes and resource usage. It schedules jobs and tasks of HDFS
* MapReduce – A framework that helps programs do the parallel computation on data.
  * The map task takes input data and converts it into a dataset that can be computed in key value pairs.
  * The output of the map task is consumed by reduce tasks to aggregate output and provide the desired result.
* Spark – An open source, distributed processing system commonly used for big data workloads.
  * Apache Spark uses in-memory caching and optimized execution for fast performance, and it supports general batch processing, streaming analytics, machine learning, graph databases, and ad hoc queries
* DAG Directed Acyclic Graph - jargon for execution plan


