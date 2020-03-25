package com.atguigu.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // http://www.baidu.com
        String line = key.toString();
        line = line + "\r\n";
        k.set(line);
        for (NullWritable value : values) {
            context.write(k, NullWritable.get());
        }
    }
}
