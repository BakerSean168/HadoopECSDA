package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BrandAgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text brand = new Text();
    private IntWritable age = new IntWritable();
    private Map<String, Integer> cache = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 初始化缓存
        cache.clear();
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        if (line.contains("brand")) {
            return;
        }
        if (fields.length == 9 && fields[6] == "electronics.smartphone") {
            System.out.println("fields: " + fields);
            String brandName = fields[6];
            int userAge = Integer.parseInt(fields[8]);

            brand.set(brandName);
            age.set(userAge);

            // 缓存中间结果
            cache.put(brandName, cache.getOrDefault(brandName, 0) + 1);

            context.write(brand, age);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 将缓存中的数据写入上下文
        for (Map.Entry<String, Integer> entry : cache.entrySet()) {
            brand.set(entry.getKey());
            age.set(entry.getValue());
            context.write(brand, age);
        }
    }
}
