package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SalesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> brandSales = new HashMap<>();
        Map<Integer, Integer> ageDistribution = new HashMap<>();
        Map<Integer, Double> timeSales = new HashMap<>();
        Map<String, Double> dateSales = new HashMap<>();

        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            if (key.toString().equals("brand")) {
                String brand = fields[0];
                double price = Double.parseDouble(fields[1]);
                brandSales.put(brand, brandSales.getOrDefault(brand, 0.0) + price);
            } else if (key.toString().equals("age")) {
                int age = Integer.parseInt(fields[0]);
                ageDistribution.put(age, ageDistribution.getOrDefault(age, 0) + 1);
            } else if (key.toString().equals("time")) {
                int hour = Integer.parseInt(fields[0]);
                double price = Double.parseDouble(fields[1]);
                timeSales.put(hour, timeSales.getOrDefault(hour, 0.0) + price);
            } else if (key.toString().equals("date")) {
                String dayType = fields[0];
                double price = Double.parseDouble(fields[1]);
                dateSales.put(dayType, dateSales.getOrDefault(dayType, 0.0) + price);
            }
        }

        if (key.toString().equals("brand")) {
            for (Map.Entry<String, Double> entry : brandSales.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
            }
        } else if (key.toString().equals("age")) {
            for (Map.Entry<Integer, Integer> entry : ageDistribution.entrySet()) {
                context.write(new Text(entry.getKey().toString()), new Text(entry.getValue().toString()));
            }
        } else if (key.toString().equals("time")) {
            for (Map.Entry<Integer, Double> entry : timeSales.entrySet()) {
                context.write(new Text(entry.getKey().toString()), new Text(entry.getValue().toString()));
            }
        } else if (key.toString().equals("date")) {
            for (Map.Entry<String, Double> entry : dateSales.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
            }
        }
    }
}