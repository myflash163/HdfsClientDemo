package com.atguigu.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean result = parseLong(line, context);
        if (!result) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    private boolean parseLong(String line, Context context) {
        String[] fields = line.split("[\\s]+");
        if(fields.length > 11){
            context.getCounter("map","true").increment(1);
            return true;
        }else{
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
