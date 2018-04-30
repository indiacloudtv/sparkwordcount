package com.cloudtv.spark.samples

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object demo {
  def main(args: Array[String]) {

    println("Spark WordCount Sample Started ...")
    val spark = SparkSession.builder()
      .master(master = "local[*]")
      .appName("Word Count Demo")
      .getOrCreate()

    val sc = spark.sparkContext
    //val sc = new SparkContext("spark://127.0.0.1:7077", "Word Count Demo")

    // creating an linesRDD by loading text file (hadoop.txt) from HDFS through Spark context

    val linesRDD = sc.textFile("hdfs://localhost:9000/cloudtv/data/mr_wordcount_input/hadoop.txt")

    val wordCounts = linesRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    println("Total words: " + wordCounts.count())

    println("All the words and occurrence count for each word: ")
    val wordCountsScala = wordCounts.collect()
    wordCountsScala.foreach(item => println("Word: " + item._1 + " --> " + "Occurrence count: " + item._2))

    println("Spark WordCount Sample Completed.")

  }
}