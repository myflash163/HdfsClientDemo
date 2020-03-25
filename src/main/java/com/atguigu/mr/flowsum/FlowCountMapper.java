package com.atguigu.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("[\\s]+");

        k.set(fields[0]);
        int length = fields.length;
        long upFlow = Long.parseLong(fields[length - 3].trim());
        long downFlow = Long.parseLong(fields[length - 2].trim());
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        //v.set(upFlow,downFlow);
        context.write(k,v);
    }
}
