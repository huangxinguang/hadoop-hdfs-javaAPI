package com.starlight.hdfs;

/**
 * @author xghuang
 * @date 2018/7/26
 * @time 8:55
 * @desc:
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsClientDemo {

    /**
     * 上传文件
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception {
        Configuration conf=new Configuration();
        URI uri = new URI("hdfs://node:9000");
        FileSystem fs=FileSystem.get(uri,conf,"root");
        //本地文件
        Path src =new Path("D:\\access.log");
        //HDFS 存放位置
        Path dst =new Path("/data");
        fs.copyFromLocalFile(src, dst);
        System.out.println("Upload to "+conf.get("fs.defaultFS"));

        //以下相当于􀩯行hdfs dfs -ls /
        FileStatus[] files = fs.listStatus(dst);
        for(FileStatus file:files) {
            System.out.println(file.getPath());
        }
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void testCreateFile() throws Exception {
        FileSystem fs=FileSystem.get(new URI("hdfs://node:9000"),new Configuration(),"root");
        //定义新文件
        Path dfs =new Path("/hdfsfile");
        //创建新文件，如果有􀑶覆􁄥(true)
        FSDataOutputStream create = fs.create(dfs, true);
        create.writeBytes("Hello,HDFS!");
    }

    /**
     * 获取文件详情
     * @throws Exception
     */
    @Test
    public void testFileLocation() throws Exception {
        FileSystem fs=FileSystem.get(new URI("hdfs://node:9000"),new Configuration(),"root");
        Path fpath=new Path("/hdfsfile");
        FileStatus filestatus = fs.getFileStatus(fpath);

        BlockLocation[] blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
        filestatus.getAccessTime();
        for(int i=0;i<blkLocations.length;i++){
            String[] hosts = blkLocations[i].getHosts();
            System.out.println("block_"+i+"_location:"+hosts[0]);
        }

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //取文件􁝺问时间，􁤄回long
        long accessTime = filestatus.getAccessTime();
        System.out.println("access:"+formatter.format(new Date(accessTime)));
        //取文件修改时间，􁤄回long
        long modificationTime = filestatus.getModificationTime();
        System.out.println("modification:"+formatter.format(new Date(modificationTime)));
        //取块大小，单位为B
        long blockSize = filestatus.getBlockSize();
        System.out.println("blockSize:"+blockSize);
        //取文件大小，单位为B
        long len = filestatus.getLen();

        System.out.println("length:"+len);
        //􁖌取文件所在用户组
        String group = filestatus.getGroup();
        System.out.println("group:"+group);
        //取文件􀪴有者
        String owner = filestatus.getOwner();
        System.out.println("owner:"+owner);
        //取文件复制数
        short replication = filestatus.getReplication();
        System.out.println("replication:"+replication);
    }

    /**
     * 创建目录
     */
    @Test
    public void testDownload() throws Exception {
        FileSystem fs=FileSystem.get(new URI("hdfs://node:9000"),new Configuration(),"root");
        //hdfs 上文件
        Path src=new Path("/hdfsfile");
        //下载到本地的文件名
        Path dst=new Path("D:/hdfsfile");
        fs.copyToLocalFile(src, dst);
    }

}
