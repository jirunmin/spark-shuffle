# Spark Shuffle算法比较

## 研究目的

比较Spark中的两种Shuffle算法：基于Hash和基于Sort。

## 研究内容

对比分析Spark中基于Hash和基于Sort的两种Shuffle算法的执行流程，探讨它们各自的优缺点及适用场景。

## 实验

### 实验环境

#### 1. 硬件环境

本实验采用火山引擎（Volcengine）ECS 集群，共包含三台实例（1 台 Master，2 台 Worker），均为 ecs.g3il.large 规格，配置如下：

**（1）计算资源**

- CPU：**2 vCPU**
- 内存：**8 GiB**

**（2）存储资源**

- 系统盘：**极速型 SSD PL0，20 GiB**
- 虽然虚拟机层 `lsblk` 显示 ROTA=1（旋转介质），但根据火山云配置，底层实际使用 SSD，因此磁盘 IO 延迟较低，有利于 Shuffle 阶段的磁盘读写性能。

**（3）网络配置**

- 外网带宽：**1 Mbps**（实例限速）
- 内网带宽：**约 1 Gbps**（火山云 VPC 虚拟交换机）
- Shuffle 过程中节点间通信主要通过内网完成，因此内网带宽对性能影响更为关键。

**（4）节点列表**

| 角色       | 实例类型           | CPU    | 内存    | 系统盘            | 操作系统         |
| -------- | -------------- | ------ | ----- | -------------- | ------------ |
| Master   | ecs.g3il.large | 2 vCPU | 8 GiB | SSD PL0 20 GiB | Ubuntu 22.04 |
| Worker 1 | ecs.g3il.large | 2 vCPU | 8 GiB | SSD PL0 20 GiB | Ubuntu 22.04 |
| Worker 2 | ecs.g3il.large | 2 vCPU | 8 GiB | SSD PL0 20 GiB | Ubuntu 22.04 |

---

#### 2. 软件环境

**（1）操作系统**

- Ubuntu **22.04.4 LTS**

**（2）Java 环境**

- OpenJDK **1.8.0_472**

**（3）Spark 版本**

- Spark **1.6.3** (Standalone 模式)

**（4）Python 环境**

- Python **3.10.12**

---

#### 3. 节点角色与服务分布

| 节点 IP      | 主机名          | 角色     | 运行服务            |
| ---------- | ------------      | ------ | --------------- |
| 172.31.0.2 | spark-master      | Master | Master |
| 172.31.0.3 | spark-worker1     | Worker | Worker          |
| 172.31.0.4 | spark-worker2     | Worker | Worker          |

---

#### 4. Spark 资源配置

| 项目            | 数值      |
| ------------- | ------- |
| Worker CPU 核数 | 2 cores |
| Worker 内存     | 4 GB    |
| Shuffle 服务端口  | 7337    |
| Spark UI 端口   | 8080    |

### 实验负载

#### 1. 数据集说明

本实验采用自定义脚本生成结构化 CSV 数据集，每条记录由三列整数组成：

```
id,rand1,rand2
```

其中：

* `id` 为行号，单调递增；
* `rand1`、`rand2` 为 0 $\sim$ 1,000,000 的均匀随机整数。

这种结构能够方便构造基于键（key）的 Shuffle 工作负载，用于测试 Hash Shuffle 与 Sort Shuffle 在不同数据规模下的性能差异。

根据实验需求，生成了六个不同规模的数据集，分别包含 2M、4M、8M、16M、32M、64M 条记录，文件大小从约 41MB 到 1.4GB 不等：

| 数据集名称         | 行数          | 文件大小 |
| ------------- | ----------- | ------- |
| data_2m.csv   | 2,000,000   | 41 MB  |
| data_4m.csv  | 4,000,000  | 83 MB  |
| data_8m.csv  | 8,000,000  | 166 MB  |
| data_16m.csv  | 16,000,000  | 337 MB  |
| data_32m.csv | 32,000,000 | 685 MB  |
| data_64m.csv | 64,000,000 | 1.4 GB  |

为了保证 Worker 节点能够本地访问数据，所有数据集均被分发到 Spark Standalone 集群的所有节点（Master + Worker）相同路径 `/home/spark/data` 下。

---

#### 2. 工作负载

为了对比 Hash Shuffle 与 Sort Shuffle 的性能，本实验选取两类典型的 Shuffle-heavy 操作：**聚合类 Shuffle（reduceByKey）** 和 **排序类 Shuffle（sortBy）**。这两类操作都会触发 Spark 在不同 Worker 之间重新分发数据，从而充分体现 Shuffle 算法性能差异。

---

**（1）聚合类工作负载：reduceByKey**

该工作负载最能反映 Spark 在数据聚合场景的 Shuffle 行为。

处理流程如下：

```scala
val data = sc.textFile(input)
val pairs = data.map { line =>
    val arr = line.split(",")
    (arr[1].toInt, 1)      // 使用 rand1 作为 key，保证 key 分布均匀
}
val result = pairs.reduceByKey(_ + _)
result.saveAsTextFile(output)
```

这样处理的特点是：

- `reduceByKey` 会对相同 key 的 value 进行聚合；
- 会触发 Shuffle，将相同 key 的记录移动到同一 Reducer；
- 使用 CSV 中均匀分布的 `rand1` 作为 key，可有效避免数据倾斜；
- 是 Shuffle 性能评测中最典型的 benchmark。

此 workload 用于测试 **聚合类 Shuffle** 场景下的执行时间与 Shuffle I/O 性能。

---

**（2）排序类工作负载：sortBy**

用于测试 Spark 在全局排序场景下的 Shuffle 行为。

处理流程：

```scala
val data = sc.textFile(input)
val sorted = data.sortBy { line =>
    line.split(",")(0).toInt   // 按 id 排序
}
sorted.saveAsTextFile(output)
```

这样处理的特点是：

- `sortBy` 必然触发全局 Shuffle，将所有记录重新分布到有序分区；
- 整体 Shuffle I/O 更大，能体现 Sort Shuffle 在排序场景的优势。

此 workload 用于测试 **排序类 Shuffle** 的性能特征。

---

#### 3. Shuffle 策略测试

我们为上述两个 workload 分别测试：

- Hash Shuffle + reduce
- Sort Shuffle + reduce
- Hash Shuffle + sort
- Sort Shuffle + sort

这四种组合覆盖了 Spark 中最常见的 Shuffle 场景。

## 实验步骤

本实验基于三节点 Spark Standalone 集群（1 Master + 2 Worker）完成。实验主要包括：环境部署、数据集生成、测试程序打包、分布式作业运行以及结果记录。关键步骤如下：

#### 1. 部署 Spark Standalone 集群

在三台云服务器上安装 JDK 1.8 与 Spark 1.6.3，并配置 Master 与 Worker 的启动脚本。

- 在 master 节点执行：

```bash
$ jps
6944 Jps
1575 Master
```

- 在 worker1 节点执行：

```bash
$ jps
3558 Jps
1177 Worker
```

- 在 worker2 节点执行：

```bash
$ jps
1178 Worker
3563 Jps
```

如下图所示：

![](images/1.png)

Spark 集群启动成功后，可通过浏览器访问 Spark UI：

```
ssh -L 18081:localhost:8080 spark-master
http://localhost:18081
```

打开后可以看出集群已成功部署：

![](images/2.png)

---

#### 2. 生成不同规模的数据集

根据实验要求，使用 Python 脚本自动生成六个不同规模的 CSV 数据集：

| 数据集          | 行数         | 文件大小（约） |
| ------------ | ---------- | ------- |
| data_2m.csv  | 2,000,000  | 41 MB   |
| data_4m.csv  | 4,000,000  | 83 MB   |
| data_8m.csv  | 8,000,000  | 166 MB  |
| data_16m.csv | 16,000,000 | 337 MB  |
| data_32m.csv | 32,000,000 | 685 MB  |
| data_64m.csv | 64,000,000 | 1.36 GB |

生成脚本示例：

```bash
python3 gen_data.py 8000000  data_8m.csv
```

所有数据集均存放于：

```
/home/spark/data/
```

可截图数据目录内容，证明数据已准备就绪。

---

#### 3. 编写并打包 Shuffle 测试程序

实验测试的主体程序 ShuffleTest 使用 Scala 编写，包含两类工作负载：

* **reduce**：使用 reduceByKey 触发 Shuffle
* **sort**：使用 sortBy 触发全局排序

并按照如下方式加入可读性极强的实验名称：

```
ShuffleTest-[shuffleManager]-[workload]-[datasetSize]
```

使用 sbt 进行构建：

```bash
./build.sh
```

构建后生成 JAR 文件：

```
target/scala-2.10/shuffle-test_2.10-0.1.jar
```

可截图 build 成功、JAR 生成位置等内容。

---

#### 4. 运行 run_all.sh 自动化实验脚本

实验使用统一脚本自动执行所有组合：

* Shuffle manager：`hash`、`sort`
* Workload：`reduce`、`sort`
* Dataset：2m–64m

脚本示例：

```bash
./run_all.sh
```

脚本会依次提交 24 个实验任务，并将输出写入：

```
/home/spark/results/
```

可截图终端运行过程，证明实验确实在用户 spark 环境中执行。

---

#### 5. 查看作业执行情况

所有作业执行完成后，可在 Spark History Server 查看任务执行情况：

访问地址：

```
http://<master-ip>:18080/
```

在 Completed Applications 页面可看到如下任务名称：

```
ShuffleTest-hash-reduce-8m
ShuffleTest-sort-sort-16m
ShuffleTest-hash-sort-32m
...
```

均显示为：

```
State: FINISHED
```

可截图该页面，证明实验真实执行且全部成功完成。

---

#### 6. 检查输出结果是否正确生成

使用如下命令检查所有结果目录是否包含 part 文件：

```bash
for d in /home/spark/results/*; do
  echo "$d:"
  ls $d/part-*
done
```

所有结果均成功写出，可进一步截图证明数据已生成。

---


