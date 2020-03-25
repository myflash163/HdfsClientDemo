package com.atguigu.mr.index;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //atguigu--a.txt 3
        //       --b.txt 2
        StringBuffer sb = new StringBuffer();
        for (Text value : values) {
            sb.append(value.toString().replace("\t", "-->") + "\t");
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}
