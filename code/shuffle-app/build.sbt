name := "shuffle-test"

version := "0.1"

// Spark 1.6.3 对应的 Scala 版本：2.10.x
scalaVersion := "2.10.6"

// Spark 1.6.3 依赖，标记为 provided（运行时用集群上的 Spark）
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"
)
