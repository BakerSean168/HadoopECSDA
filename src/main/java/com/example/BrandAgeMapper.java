package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BrandAgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text brand = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // 跳过标题行
        if (line.contains("brand")) {
            return;
        }

        // 确保字段长度正确
        if (fields.length == 6 && "electronics.smartphone".equals(fields[2])) {
            String brandName = fields[3];

            brand.set(brandName);


            context.write(brand, new IntWritable(1));
        }
    }
}