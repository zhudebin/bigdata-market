package com.zmyuan.bg.spark.hdfs

import java.util.Date

import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{JobConf, JobContext, TextInputFormat}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhudebin on 16/4/19.
  */
object HdfsTest1 {
  val path = "/Users/zhudebin/Documents/iworkspace/opensource/bigdata_market/spark/doc/text.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[6]")
      .setAppName("hdfs test")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.hadoop.validateOutputSpecs", "false")
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
    val lines = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 10)
    lines.saveAsSequenceFile("hdfs://e160:8020/user/ds/test2/test1.seq")
  }

  def pairHdfsText(sc:SparkContext): Unit = {
    val lines = sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 10)
    val pairRDD = lines.map(t2 => {
      val line = t2._2.toString.split(" ")
      (line(0), line(1))
    })

    // 处理 Append，Overwrite，ErrorIfExists，Ignore
    // 1. Append          1>判断是否有文件夹存在 2> 存在,删除里面 _temp文件夹  3> 设置不需要验证
    // 2. Overwrite       1>删除目标文件夹
    // 3. ErrorIfExists   1>判断是否有文件夹存在 2> 存在 抛异常,squidflow 运行异常
    // 4. Ignore          1>判断是否有文件夹存在 2> 存在, 不执行落地操作

    val processType = 1

    val conf = sc.hadoopConfiguration

    prepareSave(processType, conf, path = "hdfs://e160:8020/user/ds/test3/test1.seq")
    pairRDD.saveAsNewAPIHadoopDataset(conf)

  }

  def prepareSave(processType : Int, conf:Configuration, path:String): Boolean = {

    processType match {
      case 1 => {
        // 1
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if(exist) {
          if(fileSystem.exists(new Path(path + "/_temporary"))) {
            fileSystem.delete(new Path(path + "/_temporary"), true)
          }
        }
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        // 2

        // 3
      }
      case 2 => ()
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if (exist) {
          fileSystem.delete(filePath, true)
        }
      case 3 => ()
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if(exist) throw new RuntimeException("exit file")
      case 4 => ()
        val filePath = new Path(path)
        val fileSystem = filePath.getFileSystem(conf)
        val exist = fileSystem.exists(filePath)
        if(exist) {
          return true
        }
      case _ => ()
    }

    false
  }

}
