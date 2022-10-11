# Spark for Java Developers course

[Udemy - Spark for Java Developers](https://www.udemy.com/course/apache-spark-for-java-developers)

[RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
* Resilient Distributed Datasets (RDDs) -> RDD Operations
  * [Transformactions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)
  * [Actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)

[Spark SQL, Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html)

[Static functions class and available methods](https://spark.apache.org/docs/1.5.0/api/java/org/apache/spark/sql/functions.html)

[Class RelationalGroupedDataset - a set of methods for aggregations on a DataFrame, created by groupBy, cube or rollup (and also pivot)](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/RelationalGroupedDataset.html)

[Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

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


