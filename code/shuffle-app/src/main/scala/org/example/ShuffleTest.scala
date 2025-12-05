package org.example

import org.apache.spark.{SparkConf, SparkContext}

object ShuffleTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: ShuffleTest <input> <output> <opType>")
      System.err.println("  opType: reduce | group")
      System.exit(1)
    }

    val Array(inputPath, outputPath, opType) = args

    // 不在代码里写 master，交给 spark-submit 的 --master
    val conf = new SparkConf().setAppName(s"ShuffleTest-$opType")
    val sc   = new SparkContext(conf)

    // 读你之前生成的 "k,v" 文本数据
    val data = sc.textFile(inputPath)
      .map { line =>
        val Array(k, v) = line.split(",")
        (k.toInt, v.toInt)
      }

    // 两种典型 Shuffle 操作：reduceByKey vs groupByKey
    val result = opType match {
      case "reduce" =>
        // 本地先聚合再 Shuffle，网络开销相对小
        data.reduceByKey(_ + _)

      case "group" =>
        // 直接把所有 value 拉到一起，Shuffle 压力更大
        data.groupByKey().mapValues(_.sum)

      case other =>
        throw new IllegalArgumentException(s"Unsupported opType: $other")
    }

    // 触发执行
    result.saveAsTextFile(outputPath)

    sc.stop()
  }
}
