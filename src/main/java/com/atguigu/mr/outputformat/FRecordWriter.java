package com.atguigu.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosatguigu;
    FSDataOutputStream fosother;

    public FRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            fosatguigu = fs.create(new Path("g:/output/atguigu.log"));
            fosother = fs.create(new Path("g:/output/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("atguigu")) {
            fosatguigu.write(key.toString().getBytes());
        } else {
            fosother.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fosatguigu);
        IOUtils.closeStream(fosother);
    }
}
