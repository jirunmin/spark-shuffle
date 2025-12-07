import org.apache.spark.{SparkConf, SparkContext}

object ShuffleTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println(
        "Usage: ShuffleTest <input> <output> <shuffleManager> <workload> <sizeTag>"
      )
      System.err.println("Workload = reduce | sort | sort-skew")
      System.exit(1)
    }

    val inputPath  = args(0)
    val outputPath = args(1)
    val shuffleMgr = args(2)      // hash / sort
    val workload   = args(3)      // reduce / sort / sort-skew
    val sizeTag    = args(4)      // 2m / 4m / 8m / ...

    val appName = s"ShuffleTest-${shuffleMgr}-${workload}-${sizeTag}"

    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.shuffle.manager", shuffleMgr)

    val sc = new SparkContext(conf)

    // 读取 CSV 文本数据
    val data = sc.textFile(inputPath)

    workload match {

      // ==========================
      // A. reduce（按第二列，均匀）
      // ==========================
      case "reduce" =>
        val pairs = data.map { line =>
          val arr = line.split(",")
          val key = arr(1).toInt          // 第二列，均匀
          (key, 1)
        }
        val result = pairs.reduceByKey(_ + _)
        result.saveAsTextFile(outputPath)

      // ==========================
      // B. sort（按第一列 id，递增）
      // ==========================
      case "sort" =>
        val sorted = data.sortBy { line =>
          val arr = line.split(",")
          arr(0).toInt                     // 第一列 id
        }
        sorted.saveAsTextFile(outputPath)

      // ==========================
      // C. sort-skew（按第三列 skew_key，强倾斜）
      // ==========================
      case "sort-skew" =>
        val sorted = data.sortBy { line =>
          val arr = line.split(",")
          arr(2).toInt                     // 第三列：专门造倾斜
        }
        sorted.saveAsTextFile(outputPath)

      case _ =>
        sys.error("Unknown workload: use reduce | sort | sort-skew")
    }

    sc.stop()
  }
}
