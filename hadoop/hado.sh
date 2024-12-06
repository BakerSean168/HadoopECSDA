#!/bin/bash

# 清理并打包项目
mvn clean package

# 删除 HDFS 上的输出目录
hadoop fs -rm -r /user/root/output

# 运行 Hadoop 作业
hadoop jar target/hademo-1.0-SNAPSHOT.jar com.example.SalesAnalysis input/ output/
