package com.example;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BrandAgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text brand = new Text();
    private IntWritable age = new IntWritable();
    private static final Log Log = LogFactory.getLog(BrandAgeMapper.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // 跳过标题行
        if (line.contains("brand")) {
            return;
        }

        // 确保字段长度正确
        if (fields.length == 6 && "electronics.smartphone".equals(fields[2])) {
            Log.info("Processing record: " + line);
            String brandName = fields[3];
            int userAge = Integer.parseInt(fields[5]);

            brand.set(brandName);
            age.set(userAge);

            context.write(brand, age);
        }
    }
}