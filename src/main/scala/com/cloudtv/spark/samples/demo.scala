package com.cloudtv.spark.samples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object demo {
  def main(args: Array[String]) {

    println("Spark WordCount Sample Started ...")
    val sc = new SparkContext("spark://127.0.0.1:7077", "Word Count Demo")

    // creating an linesRDD by loading text file (hadoop.txt) from HDFS through Spark context

    val linesRDD = sc.textFile("hdfs://localhost:9000/cloudtv/data/mr_wordcount_input/hadoop.txt")

    val wordCounts = linesRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    wordCounts.count()

    wordCounts.collect()

    println("Spark WordCount Sample Completed.")

  }
}