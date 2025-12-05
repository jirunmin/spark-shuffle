import org.apache.spark.{SparkConf, SparkContext}

object ShuffleTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println(
        "Usage: ShuffleTest <input> <output> <shuffleManager> <workload> <sizeTag>"
      )
      System.err.println("Example: Hash Reduce 8m => hash reduce 8m")
      System.exit(1)
    }

    val inputPath  = args(0)
    val outputPath = args(1)
    val shuffleMgr = args(2)    // hash / sort
    val workload   = args(3)    // reduce / sort
    val sizeTag    = args(4)    // 2m / 4m / 8m / …

    val appName = s"ShuffleTest-${shuffleMgr}-${workload}-${sizeTag}"

    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.shuffle.manager", shuffleMgr)

    val sc = new SparkContext(conf)

    // 读取 CSV 文本数据
    val data = sc.textFile(inputPath)

    // ========== Workload A: reduceByKey ==========
    workload match {
      case "reduce" =>
        val pairs = data.map { line =>
          val arr = line.split(",")
          val key = arr(1).toInt
          (key, 1)
        }
        val result = pairs.reduceByKey(_ + _)
        result.saveAsTextFile(outputPath)

      // ========== Workload B: sortBy ==========
      case "sort" =>
        val sorted = data.sortBy { line =>
          val arr = line.split(",")
          arr(0).toInt
        }
        sorted.saveAsTextFile(outputPath)

      case _ =>
        sys.error("Unknown workload: use reduce or sort")
    }

    sc.stop()
  }
}
