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

#### 代码
[Q1SumDeptSalary.java](/src/com/elon33/mr1/Q1SumDeptSalary.java "点击此处")

---
### 例子2：求各个部门的人数和平均工资
#### 问题分析
求各个部门的人数和平均工资，需要得到各部门工资总数和部门人数，通过两者相除获取各部门平均工资。首先和问题1类似在Mapper的Setup阶段缓存部门数据，然后在Mapper阶段抽取出部门编号和员工工资，利用缓存部门数据把部门编号对应为部门名称，接着在Shuffle阶段把传过来的数据处理为部门名称对应该部门所有员工工资的列表，最后在Reduce中按照部门归组，遍历部门所有员工，求出总数和员工数，输出部门名称和平均工资。
#### 处理流程图
![求各个部门的人数和平均工资处理流程图](https://i.imgur.com/PLjM1Du.jpg)
#### 代码
[Q2DeptNumberAveSalary.java](/src/com/elon33/mr1/Q2DeptNumberAveSalary.java "点击此处")

---
### 例子3：求每个部门最早进入公司的员工姓名
#### 问题分析
求每个部门最早进入公司员工姓名，需要得到各部门所有员工的进入公司日期，通过比较获取最早进入公司员工姓名。首先和问题1类似在Mapper的Setup阶段缓存部门数据，然后Mapper阶段抽取出key为部门名称（利用缓存部门数据把部门编号对应为部门名称），value为员工姓名和进入公司日期，接着在Shuffle阶段把传过来的数据处理为部门名称对应该部门所有员工+进入公司日期的列表，最后在Reduce中按照部门归组，遍历部门所有员工，找出最早进入公司的员工并输出。
#### 处理流程图
![求每个部门最早进入公司的员工姓名处理流程图](https://i.imgur.com/3nWTvtX.jpg)
#### 代码
[Q3DeptEarliestEmp.java](/src/com/elon33/mr1/Q3DeptEarliestEmp.java "点击此处")

---
### 例子4：求各个城市的员工的总工资
#### 问题分析
求各个城市员工的总工资，需要得到各个城市所有员工的工资，通过对各个城市所有员工工资求和得到总工资。首先和测试例子1类似在Mapper的Setup阶段缓存部门对应所在城市数据，然后在Mapper阶段抽取出key为城市名称（利用缓存数据把部门编号对应为所在城市名称），value为员工工资，接着在Shuffle阶段把传过来的数据处理为城市名称对应该城市所有员工工资，最后在Reduce中按照城市归组，遍历城市所有员工，求出工资总数并输出。
#### 处理流程图
![求各个城市的员工的总工资处理流程图](https://i.imgur.com/4onxKWP.jpg)
#### 代码
[Q4SumCitySalary.java](/src/com/elon33/mr1/Q4SumCitySalary.java "点击此处")

---
### 例子5：列出工资比上司高的员工姓名及其工资
#### 问题分析
求工资比上司高的员工姓名及工资，需要得到上司工资及上司所有下属员工，通过比较他们工资高低得到比上司工资高的员工。在Mapper阶段输出经理数据和员工对应经理表数据，其中经理数据key为员工编号、value为"M，该员工工资"，员工对应经理表数据key为经理编号、value为"E，该员工姓名，该员工工资"；然后在Shuffle阶段把传过来的经理数据和员工对应经理表数据进行归组，如编号为7698员工，value中标志M为自己工资，value中标志E为其下属姓名及工资；最后在Reduce中遍历比较员工与经理工资高低，输出工资高于经理的员工。
#### 处理流程图
![列出工资比上司高的员工姓名及其工资处理流程图](https://i.imgur.com/OJd59kU.jpg)
#### 代码
[Q5EarnMoreThanManager.java](/src/com/elon33/mr1/Q5EarnMoreThanManager.java "点击此处")

---
### 例子6：列出工资比公司平均工资要高的员工姓名及其工资
#### 问题分析
求工资比公司平均工资要高的员工姓名及工资，需要得到公司的平均工资和所有员工工资，通过比较得出工资比平均工资高的员工姓名及工资。这个问题可以分两个作业进行解决，先求出公司的平均工资，然后与所有员工进行比较得到结果；也可以在一个作业进行解决，这里就得使用作业setNumReduceTasks方法，设置Reduce任务数为1，保证每次运行一个reduce任务，在该例子中，只需要一个reduce任务就可以处理完数据，从而能先求出平均工资，然后进行比较得出结果。

在Mapper阶段输出两份所有员工数据，其中一份key为0、value为该员工工资，另外一份key为1、value为"该员工姓名 ,员工工资"；然后在Shuffle阶段把传过来数据按照key进行归组，在该任务中有key值为0和1两组数据；最后在Reduce中对key值0的所有员工求工资总数和员工数，获得平均工资；对key值1，比较员工与平均工资的大小，输出比平均工资高的员工和对应的工资。
#### 处理流程图
![列出工资比公司平均工资要高的员工姓名及其工资处理流程图](https://i.imgur.com/XzYfv1y.jpg)
#### 代码
[Q6HigherThanAveSalary.java](/src/com/elon33/mr1/Q6HigherThanAveSalary.java "点击此处")

---
### 例子7：列出名字以J开头的员工姓名及其所属部门名称
#### 问题分析
求名字以J开头的员工姓名机器所属部门名称，只需判断员工姓名是否以J开头。首先和问题1类似在Mapper的Setup阶段缓存部门数据，然后在Mapper阶段判断员工姓名是否以J开头，如果是抽取出员工姓名和员工所在部门编号，利用缓存部门数据把部门编号对应为部门名称，转换后输出结果。
#### 处理流程图
![列出名字以J开头的员工姓名及其所属部门名称处理流程图](https://i.imgur.com/en42aUb.jpg)
#### 代码
[Q7NameDeptOfStartJ.java](/src/com/elon33/mr1/Q7NameDeptOfStartJ.java "点击此处")

---
### 例子8：列出工资最高的头三名员工姓名及其工资
#### 问题分析
求工资最高的头三名员工姓名及工资，可以通过冒泡法得到。在Mapper阶段输出经理数据和员工对应经理表数据，其中经理数据key为0值、value为"员工姓名，员工工资"；最后在Reduce中通过冒泡法遍历所有员工，比较员工工资多少，求出前三名。
#### 处理流程图
![列出工资最高的头三名员工姓名及其工资处理流程图](https://i.imgur.com/fjcyHu8.jpg)
#### 代码
[Q8SalaryTop3Salary.java](/src/com/elon33/mr1/Q8SalaryTop3Salary.java "点击此处")

---
### 例子9：将全体员工按照总收入（工资+提成）从高到低排列
#### 问题分析
求全体员工总收入降序排列，获得所有员工总收入并降序排列即可。在Mapper阶段输出所有员工总工资数据，其中key为员工总工资、value为员工姓名，在Mapper阶段的最后会先调用job.setPartitionerClass对数据进行分区，每个分区映射到一个reducer，每个分区内又调用job.setSortComparatorClass设置的key比较函数类排序。由于在本作业中Map的key只有0值，故能实现对所有数据进行排序。
#### 处理流程图
![将全体员工按照总收入（工资+提成）从高到低排列处理流程图](https://i.imgur.com/eJ6CpgQ.jpg)
#### 代码
[Q9EmpSalarySort.java](/src/com/elon33/mr1/Q9EmpSalarySort.java "点击此处")

---
### 例子10：求任何两名员工信息传递所需要经过的中间节点数
#### 问题分析
该公司所有员工可以形成入下图的树形结构，求两个员工的沟通的中间节点数，可转换在员工树中求两个节点连通所经过的节点数，即从其中一节点到汇合节点经过节点数加上另一节点到汇合节点经过节点数。例如求M到Q所需节点数，可以先找出M到A经过的节点数，然后找出Q到A经过的节点数，两者相加得到M到Q所需节点数。

![求M到Q所需节点数](https://i.imgur.com/n8Ldt3q.jpg)
 
在作业中首先在Mapper阶段所有员工数据，其中经理数据key为0值、value为"员工编号，员工经理编号"，然后在Reduce阶段把所有员工放到员工列表和员工对应经理链表Map中，最后在Reduce的Cleanup中按照上面说所算法对任意两个员工计算出沟通的路径长度并输出。
#### 处理流程图
![求任何两名员工信息传递所需要经过的中间节点数处理流程图](https://i.imgur.com/d8b0zB7.jpg)
#### 代码
[Q10MiddlePersonsCountForComm.java](/src/com/elon33/mr1/Q10MiddlePersonsCountForComm.java "点击此处")