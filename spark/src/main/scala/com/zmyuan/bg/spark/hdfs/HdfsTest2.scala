package com.zmyuan.bg.spark.hdfs

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputCommitter, JobConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * 测试落地到hdfs
  *
  * Created by zhudebin on 16/4/29.
  */
object HdfsTest2 {

  def main(args: Array[String]) {
    test4()
  }

  def test1(): Unit = {
    val config = new SparkConf()
      .setMaster("local[4]")
      .setAppName("testhdfs")
    //      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(config)

    sc.parallelize(Seq(1 to 200), 4).map(i => {
      i + " hello"
    }).saveAsTextFile("hdfs://192.168.137.102:8020/testDir/testhdfs3")
  }

  def test2(): Unit = {
    val config = new SparkConf()
      .setMaster("local[4]")
      .setAppName("testhdfs")
          .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(config)

    sc.parallelize(Seq(1 to 200), 4).map(i => {
      i + " hello"
    }).saveAsTextFile("hdfs://192.168.137.102:8020/testDir/testhdfs3")
  }

  def test3(): Unit = {

    val path = "hdfs://192.168.137.102:8020/testDir/testhdfs4"

    val config = new SparkConf()
      .setMaster("local[4]")
      .setAppName("testhdfs")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Seq(1 to 200), 4).map(i => {
      i + " hello"
    })

    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = rdd.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    val pairRDD = RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      pairRDD.saveAsHadoopFile[CustomTextOutputFormat[NullWritable, Text]](path)

    val jobConf = new JobConf(sc.hadoopConfiguration)

    jobConf.setOutputCommitter(null)

    pairRDD.saveAsHadoopDataset(jobConf)
  }

  def test4(): Unit = {
    val path = "hdfs://192.168.137.102:8020/testDir/testhdfs4"

    val config = new SparkConf()
      .setMaster("local[4]")
      .setAppName("testhdfs")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Seq(1 to 200), 4).map(i => {
      i + " hello"
    })

    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = rdd.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    val pairRDD = RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)

    val jobConf = new JobConf(sc.hadoopConfiguration)

//    jobConf.set("mapred.output.committer.class", "com.zmyuan.bg.spark.hdfs.CustomFileOutputCommitter")
    jobConf.setOutputCommitter(classOf[CustomFileOutputCommitter])
    jobConf.setOutputKeyClass(classOf[NullWritable])
    jobConf.setOutputValueClass(classOf[Text])
    jobConf.set("mapreduce.output.fileoutputformat.outputdir", path)
    pairRDD.saveAsHadoopDataset(jobConf)
  }
}
