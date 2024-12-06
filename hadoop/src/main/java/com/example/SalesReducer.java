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
        Map<String, Double> localSales = new HashMap<>();

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
            } else if (key.toString().equals("local")) {
                String local = fields[0];
                double price = Double.parseDouble(fields[1]);
                localSales.put(local, localSales.getOrDefault(local, 0.0) + price);
            }
        }

        if (key.toString().equals("brand")) {
            context.write(new Text("brand,sales"), null); // CSV header
            for (Map.Entry<String, Double> entry : brandSales.entrySet()) {
                context.write(new Text(entry.getKey() + "," + entry.getValue()), null);
            }
        } else if (key.toString().equals("age")) {
            context.write(new Text("age,count"), null); // CSV header
            for (Map.Entry<Integer, Integer> entry : ageDistribution.entrySet()) {
                context.write(new Text(entry.getKey() + "," + entry.getValue()), null);
            }
        } else if (key.toString().equals("time")) {
            context.write(new Text("hour,sales"), null); // CSV header
            for (Map.Entry<Integer, Double> entry : timeSales.entrySet()) {
                context.write(new Text(entry.getKey() + "," + entry.getValue()), null);
            }
        } else if (key.toString().equals("date")) {
            context.write(new Text("day_type,sales"), null); // CSV header
            for (Map.Entry<String, Double> entry : dateSales.entrySet()) {
                context.write(new Text(entry.getKey() + "," + entry.getValue()), null);
            }
        } else if (key.toString().equals("local")) {
            context.write(new Text("local,sales"), null); // CSV header
            for (Map.Entry<String, Double> entry : localSales.entrySet()) {
                context.write(new Text(entry.getKey() + "," + entry.getValue()), null);
            }
        }
    }
}