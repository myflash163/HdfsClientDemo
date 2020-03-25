package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSIO {
    /**
     * 文件上传
     */
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        FileInputStream fis = new FileInputStream(new File("g:/banzhang.txt"));
        FSDataOutputStream fos = fs.create(new Path("/banzhang.txt"));
        IOUtils.copyBytes(fis, fos, conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 文件上传
     */
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        FSDataInputStream fis = fs.open(new Path("/banzhang.txt"));
        FileOutputStream fos = new FileOutputStream(new File("g:/banhua.txt"));
        IOUtils.copyBytes(fis, fos, conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 下载第一块
     */
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.0.tar.gz"));
        FileOutputStream fos = new FileOutputStream(new File("g:/hadoop-2.10.0.tar.gz.part1"));
        //流的拷贝，只拷贝128M
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 下载第2块
     */
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.0.tar.gz"));
        fis.seek(1024 * 1024 * 128);
        FileOutputStream fos = new FileOutputStream(new File("g:/hadoop-2.10.0.tar.gz.part2"));
        IOUtils.copyBytes(fis, fos, conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
