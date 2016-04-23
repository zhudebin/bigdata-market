package com.zmyuan.bg.spark.hdfs

import java.util.Date

import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhudebin on 16/4/19.
  */
object HdfsTest1 {
  val path = "/Users/zhudebin/Documents/iworkspace/opensource/bigdata_market/spark/doc/text.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("hdfs test")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
//    val lines = sc.hadoopFile[LongWritable, Text,TextInputFormat]("/Users/zhudebin/Documents/iworkspace/opensource/bigdata_market/spark/doc/text.txt", 2)

    hdfsSequence(sc)
  }

  def hdfsText(sc:SparkContext): Unit = {
    val lines = sc.textFile(path, 2)

    val count = lines.count()
    println("count=" + count)
    lines.collect().foreach(line => {
      println(line)
    })


    val lines2 = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 2)
    val count2 = lines2.count()
    println("count2=" + count2)
    lines2.collect().foreach(t2 => {
      println(t2._1.get().toString + "," + t2._2.toString)
    })

    lines2.foreach(t2 => {
      println("---- " +t2._1.get() + "," + t2._2.toString)
    })

    /**
      * mapreduce.output.textoutputformat.separator  分隔符
      *
      *
      */
    println("====" +CSVFormat.DEFAULT.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
    println("====" +CSVFormat.EXCEL.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"", new Date()))
    println("====" +CSVFormat.MYSQL.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
    println("====" +CSVFormat.RFC4180.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
    println("====" +CSVFormat.TDF.format("hello", "aaa\"/aa", "b,\"\"\"\"\"\"\"b", new Date()))
    //    lines.saveAsTextFile("hdfs://e102:8020/testDir/test1")
  }

  def hdfsSequence(sc:SparkContext) {
    val lines = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 2)
    lines.saveAsSequenceFile("hdfs://e102:8020/testDir/test3/test.seq")
  }

}
