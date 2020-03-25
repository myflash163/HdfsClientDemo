package com.atguigu.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //atguigu--a.txt 3
        //atguigu--b.txt 2

        String line = value.toString();
        String[] fields = line.split("--");
        Text k = new Text();
        Text v = new Text();
        k.set(fields[0]);
        v.set(fields[1]);
        context.write(k, v);

    }
}
