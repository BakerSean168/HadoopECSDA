package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith(",event_time")) {
            return; // Skip header
        }

        String[] fields = line.split(",");
        if (fields.length < 6) {
            return; // Skip invalid lines
        }

        String eventTime = fields[1];
        String categoryCode = fields[2];
        String brand = fields[3];
        String price = fields[4];
        String age = fields[5];

        if (categoryCode.contains("smartphone")) {
            try {
                Date date = dateFormat.parse(eventTime);
                java.util.Calendar calendar = java.util.Calendar.getInstance();
                calendar.setTime(date);
                int hour = calendar.get(java.util.Calendar.HOUR_OF_DAY);
                int dayOfWeek = calendar.get(java.util.Calendar.DAY_OF_WEEK);
                String dayType = (dayOfWeek == java.util.Calendar.SUNDAY || dayOfWeek == java.util.Calendar.SATURDAY) ? "weekend" : "weekday";

                context.write(new Text("brand"), new Text(brand + "\t" + price));
                context.write(new Text("age"), new Text(age));
                context.write(new Text("time"), new Text(hour + "\t" + price));
                context.write(new Text("date"), new Text(dayType + "\t" + price));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}