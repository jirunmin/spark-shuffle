import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random // 导入 Random 工具

object ShuffleTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: ShuffleTest <input-path> <output-path>")
      System.err.println("Note: This version creates EXTREME data skew.")
      System.exit(1)
    }

    val inputPath  = args(0)
    val outputPath = args(1)

    // 在应用程序名称中指明这是倾斜测试
    val conf = new SparkConf().setAppName(s"ShuffleTest-Skewed-Sort-Groupbykey$inputPath")
    val sc   = new SparkContext(conf)

    // 读取 CSV 文本数据
    val data = sc.textFile(inputPath)

    // // 重点修改：人造数据倾斜逻辑 (使用 map)
    // val keyed = data.map { line =>
    //   // 在这里初始化 Random 实例 (每条记录都会创建一次，代码更简洁)
    //   val randomGenerator = new Random()
      
    //   // 关键配置：80% 的概率生成 Key = 1
    //   val SKEW_PROBABILITY = 0.80 
    //   // 倾斜的 Key 空间范围（长尾 Key 的范围）
    //   val NON_HOT_KEY_RANGE = 10000 
      
    //   val key = if (randomGenerator.nextDouble() < SKEW_PROBABILITY) {
    //     // 80% 的数据集中在 Key = 1 上 (超级热点)
    //     1
    //   } else {
    //     // 剩下的 20% 数据均匀分布在 2 到 10000 的 Key 上 (长尾 Key)
    //     randomGenerator.nextInt(NON_HOT_KEY_RANGE - 1) + 2
    //   }
      
    //   // 映射成 (key, 1) 对
    //   (key, 1)
    // }
    // 优化 1: 使用 mapPartitions 而不是 map，避免每行创建对象的开销

    val keyed = data.map { line =>
      // 警告：如果数据量大，这里每处理一行都会创建一个 Random 对象，
      // 这可能会导致严重的 GC 压力和 Executor 崩溃/超时。
      val randomGenerator = new Random() 
      val SKEW_PROBABILITY = 0.80   
      val NON_HOT_KEY_RANGE = 10000 

      // 制造数据倾斜
      val key = if (randomGenerator.nextDouble() < SKEW_PROBABILITY) {
        1 // 热点 Key
      } else {
        randomGenerator.nextInt(NON_HOT_KEY_RANGE - 1) + 2
      }
      
      // 返回 (key, 1)
      (key, 1)
    }

    // 执行 reduceByKey 操作，该操作会触发 Shuffle
    // val reduced = keyed.reduceByKey(_ + _)
    val grouped = keyed.groupByKey()
    // 写出结果，顺便强制触发计算
    // 在实际测试中，建议先删除旧的输出目录以避免报错

    val result = grouped.mapValues(iter => iter.size)

    result.saveAsTextFile(outputPath)

    sc.stop()
  }
}