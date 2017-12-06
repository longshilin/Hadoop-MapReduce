# MapReduce应用案例

## 环境说明
Hadoop搭建环境：

| 虚拟机操作系统： CentOS6.3  64位，单核，1G内存 
| JDK：1.7.0_60 64位 
| Hadoop：2.4.1

MR程序编译环境：

| Eclipse IED 
| mapred.LocalJobRunner本地运行模式

## 准备测试数据

测试数据包括两个文件dept（部门）和emp（员工），其中各字段用逗号分隔：


dept文件内容：

	10,ACCOUNTING,NEW YORK
	20,RESEARCH,DALLAS
	30,SALES,CHICAGO
	40,OPERATIONS,BOSTON
emp文件内容：

	7369,SMITH,CLERK,7902,17-12月-80,800,,20
	7499,ALLEN,SALESMAN,7698,20-2月 -81,1600,300,30
	7521,WARD,SALESMAN,7698,22-2月 -81,1250,500,30
	7566,JONES,MANAGER,7839,02-4月 -81,2975,,20
	7654,MARTIN,SALESMAN,7698,28-9月 -81,1250,1400,30
	7698,BLAKE,MANAGER,7839,01-5月 -81,2850,,30
	7782,CLARK,MANAGER,7839,09-6月 -81,2450,,10
	7839,KING,PRESIDENT,,17-11月-81,5000,,10
	7844,TURNER,SALESMAN,7698,08-9月 -81,1500,0,30
	7900,JAMES,CLERK,7698,03-12月-81,950,,30
	7902,FORD,ANALYST,7566,03-12月-81,3000,,20
	7934,MILLER,CLERK,7782,23-1月 -82,1300,,10

## 应用案例
### 例子1：求各个部门的总工资
#### 问题分析
MapReduce中的join分为好几种，比如有最常见的 reduce side join、map side join和semi join 等。reduce join 在shuffle阶段要进行大量的数据传输，会造成大量的网络IO效率低下，而map side join 在处理多个小表关联大表时非常有用 。
Map side join是针对以下场景进行的优化：两个待连接表中，有一个表非常大，而另一个表非常小，以至于小表可以直接存放到内存中。这样我们可以将小表复制多份，让每个map task内存中存在一份（比如存放到hash table中），然后只扫描大表：对于大表中的每一条记录key/value，在hash table中查找是否有相同的key的记录，如果有，则连接后输出即可。为了支持文件的复制，Hadoop提供了一个类DistributedCache，使用该类的方法如下：

（1）用户使用静态方法`DistributedCache.addCacheFile()`指定要复制的文件，它的参数是文件的URI（如果是HDFS上的文件，可以这样：`hdfs://jobtracker:50030/home/XXX/file`）。JobTracker在作业启动之前会获取这个URI列表，并将相应的文件拷贝到各个TaskTracker的本地磁盘上。
				
（2）用户使用：在分布式环境`DistributedCache.getLocalCacheFiles()`/在伪分布式环境`DistributedCache.getCacheFiles()`方法获取文件目录，并使用标准的文件读写API读取相应的文件。
在下面代码中，将会把数据量小的表(部门dept）缓存在内存中，在Mapper阶段对员工部门编号映射成部门名称，该名称作为key输出到Reduce中，在Reduce中计算按照部门计算各个部门的总工资。

#### 处理流程图
![求各个部门的总工资处理流程图](https://i.imgur.com/XpWCrvb.jpg)

