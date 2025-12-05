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
| 172.31.0.2 | spark-master      | Master | Master + Worker |
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

根据实验需求，生成了六个不同规模的数据集，分别包含 8M、16M、32M、64M、128M、256M 条记录，文件大小从约 166MB 到 5.6GB 不等：

| 数据集名称         | 行数          | 文件大小 |
| ------------- | ----------- | ------- |
| data_8m.csv   | 8,000,000   | 166 MB  |
| data_16m.csv  | 16,000,000  | 337 MB  |
| data_32m.csv  | 32,000,000  | 685 MB  |
| data_64m.csv  | 64,000,000  | 1.4 GB  |
| data_128m.csv | 128,000,000 | 2.8 GB  |
| data_256m.csv | 256,000,000 | 5.6 GB  |

为了保证 Worker 节点能够本地访问数据，所有数据集均被分发到 Spark Standalone 集群的所有节点（Master + Worker）相同路径 `/home/spark/data` 下。

---

#### 2. 工作负载

为了对比 Hash Shuffle 与 Sort Shuffle 的性能，本实验选取两类典型的 Shuffle-heavy 操作：**聚合类 Shuffle（reduceByKey）** 和 **排序类 Shuffle（sortBy）**。这两类操作都会触发 Spark 在不同 Worker 之间重新分发数据，从而充分体现 Shuffle 算法性能差异。

---

### **（1）聚合任务：reduceByKey（基于 rand1 分组）**

读取数据后，以第二列 `rand1` 作为 key 进行计数聚合：

```python
rdd = sc.textFile("file:///home/spark/data/data_XX.csv")
pairs = rdd.map(lambda line: (line.split(",")[1], 1))
out = pairs.reduceByKey(lambda a, b: a + b)
out.count()
```

特点：

* `rand1` 的取值范围较小（0～1,000,000），使得 **重复 key 数量巨大**；
* 会产生大量的 Shuffle 数据，用于聚合相同 key；
* 对 **Hash Shuffle 与 Sort Shuffle 的性能差距较敏感**。

此任务主要用于评估两种 Shuffle 算法在 **聚合类操作** 下的执行时间与 Shuffle Read/Write 大小。

---

### **（2）排序任务：sortBy（基于 id 排序）**

以第一列 `id` 为 key 进行排序：

```python
rdd = sc.textFile("file:///home/spark/data/data_XX.csv")
out = rdd.sortBy(lambda x: int(x.split(",")[0]))
out.count()
```

特点：

* `id` 单调递增，排序操作会触发完整的 Shuffle 阶段；
* Sort Shuffle 在排序场景中更具优势；
* Hash Shuffle 在排序下会产生额外步骤，性能可能更弱。

此任务主要用于评估两种 Shuffle 算法在 **排序类操作** 下的表现差异。

---

## **3. Shuffle 模式切换方法**

两种 Shuffle 模式通过 Spark 配置切换：

### ✔ Hash Shuffle

```
--conf spark.shuffle.manager=hash
```

### ✔ Sort Shuffle（默认）

```
--conf spark.shuffle.manager=sort
```

为了保持实验可重复性，所有其他参数保持一致，仅更换 Shuffle 模式。

---

