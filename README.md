# E-commerce sales data analysis

基于mapreduce的电商销售数据分析

# 预期目标

预期获取各类手机品牌的销售占比以及消费人群的年龄分布，  
分析不同时间段内的销售额变化，了解销售的高峰和低谷时段,  
对比不同日期（如工作日与周末、节假日与非节假日）的销售情况，以识别季节性或周期性趋势,  
统计不同地区的销售额占比,  
并进行可视化成果展示。  

# 项目主要结构

big-data-project/  
├── data/  
│   └── ECS.csv  
├── hadoop/  
│   ├── src/  
│   │   ├── main/  
│   │   │   └── java/  
│   │   │       └── com/  
│   │   │           └── example/  
│   │   │               ├── SalesMapper.java  
│   │   │               ├── SalesReducer.java  
│   │   │               └── SalesAnalysis.java  
│   ├── hado.sh  
│   └── pom.xml  
├── visualization/  
│   ├── visualize.py  
│   └── requirements.txt  
├── startHadoopService.sh  
└── README.md  

## 文件说明

hado.sh 为一键编译运行脚本  
startHadoop.sh 为一键启动 Hadoop 集群服务脚本  

# 项目运行

1. 已经配置好 Hadoop 本地 yarn 伪分布式集群服务
2. 运行 startHadoopService.sh，启动 Hadoop 服务
3. 进入 hadoop 文件夹执行 hado.sh，一键编译运行 mapreduce 程序
4. 运行 `hadoop fs -copyToLocal /user/[用户名]/output/ ../output/` 将分布式文件系统中的运行结果复制到本地 output 文件夹中
5. 进入 visualization 文件夹运行 `pip install -r requirements.txt` 安装依赖，然后运行 python 文件，生成可视化看板
6. 调整好图表位置后 注释掉`page.render("test.html")` 使用固定图表代码重新运行，生成最终图。

# 结果图

![result](https://github.com/user-attachments/assets/4cf45897-35fc-495e-ae5f-8ffaa98c05ec)
