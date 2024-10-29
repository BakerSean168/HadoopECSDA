package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BrandAgeReducer extends Reducer<Text, IntWritable, Text, Text> {
    private static final Log LOG = LogFactory.getLog(BrandAgeReducer.class);
    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Integer> ageDistribution = new HashMap<>();
        int totalCount = 0;

        for (IntWritable val : values) {
            int age = val.get();
            ageDistribution.put(age, ageDistribution.getOrDefault(age, 0) + 1);
            context.getCounter("AgeCounter", "Age_" + age).increment(1);
            LOG.info("Processing age: " + age);
            totalCount++;
        }

        // 将年龄分布转换为字符串
        StringBuilder ageDistributionStr = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : ageDistribution.entrySet()) {
            ageDistributionStr.append(entry.getKey()).append(":").append(entry.getValue()).append(" ");
        }

        // 输出年龄分布
        multipleOutputs.write("ageDistribution", key, new Text(ageDistributionStr.toString().trim()));

        // 输出总计数
        multipleOutputs.write("totalCount", key, new Text(String.valueOf(totalCount)));
    }
}