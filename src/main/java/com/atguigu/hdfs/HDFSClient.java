package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://hadoop102:9000");
        //FileSystem fs = FileSystem.get(conf);//-DHADOOP_USER_NAME=mys
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        fs.mkdirs(new Path("/0529/dashen/banzhang"));

        fs.close();
        System.out.println("over");

    }

    /**
     * 文件上传
     */
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        fs.copyFromLocalFile(new Path("g:/banzhang.txt"), new Path("/xiaohua.txt"));
        fs.close();
    }

    /**
     * 文件下载
     */
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        //fs.copyToLocalFile(new Path("/banhua.txt"),new Path("g:/banhua.txt"));
        fs.copyToLocalFile(false, new Path("/banhua.txt"), new Path("g:/xiaohua.txt"), true);
        fs.close();
    }

    /**
     * 文件删除
     */
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        fs.delete(new Path("/0529"), true);
        fs.close();
    }

    /**
     * 文件更名
     */
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        fs.rename(new Path("/banzhang.txt"), new Path("/yanjing.txt"));
        fs.close();
    }

    /**
     * 文件详情查看
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.print(fileStatus.getPath().getName() + "\t");//文件名称
            System.out.print(fileStatus.getPermission() + "\t");//文件权限
            System.out.println(fileStatus.getLen());//文件长度
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-------------");
        }
        fs.close();
    }

    /**
     * 判断是文件还是文件夹
     */
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "mys");
        FileStatus[] listStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        fs.close();
    }
}
