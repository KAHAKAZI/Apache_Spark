# Spark for Java Developers course

[Udemy - Spark for Java Developers](https://www.udemy.com/course/apache-spark-for-java-developers)

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


