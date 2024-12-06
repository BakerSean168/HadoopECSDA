预期目标：
预期获取各类手机品牌的销售占比以及消费人群的年龄分布，
分析不同时间段内的销售额变化，了解销售的高峰和低谷时段,
对比不同日期（如工作日与周末、节假日与非节假日）的销售情况，以识别季节性或周期性趋势,
统计不同地区的销售额占比,
并进行可视化成果展示。

项目结构
big-data-project/
├── data/
│   └── example.csv
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
├── startHadoop.sh
└── README.md