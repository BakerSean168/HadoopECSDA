package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BrandAgeReducer extends Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Map<Integer, Integer> ageDistribution = new HashMap<>();

        for (IntWritable val : values) {
            count++;
            int age = val.get();
            ageDistribution.put(age, ageDistribution.getOrDefault(age, 0) + 1);
        }

        StringBuilder result = new StringBuilder();
        result.append("Total: ").append(count).append(", Age Distribution: ").append(ageDistribution.toString());

        context.write(key, new Text(result.toString()));
    }
}