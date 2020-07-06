package com.ifchange.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;
import java.io.IOException;

public class HDFSUtil {

    private static FileSystem fs = null;

    static {
        try {
            Configuration conf = new Configuration();
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            conf.set("fs.defaultFS", "hdfs://hadoop108:8020");
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 往hdfs上传文件
     *
     */
    public void testAddFileToHdfs() throws Exception {

        // 要上传的文件所在的本地路径
        Path src = new Path("g:/redis-recommend.zip");
        // 要上传到hdfs的目标路径
        Path dst = new Path("/aaa");
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    /**
     * 从hdfs中复制文件到本地文件系统
     *
     */
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {
        fs.copyToLocalFile(new Path("/jdk-7u65-linux-i586.tar.gz"), new Path("d:/"));
        fs.close();
    }

    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {

        // 创建目录
        fs.mkdirs(new Path("/a1/b1/c1"));

        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
        fs.delete(new Path("/aaa"), true);

        // 重命名文件或文件夹
        fs.rename(new Path("/a1"), new Path("/a2"));

    }

    /**
     * 查看目录信息，只显示文件
     *
     */
    public static RemoteIterator<LocatedFileStatus> testListFiles(String path) throws FileNotFoundException, IllegalArgumentException, IOException {

        // 思考：为什么返回迭代器，而不是List之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(path), true);

//        while (listFiles.hasNext()) {
//            LocatedFileStatus fileStatus = listFiles.next();
//            System.out.println(fileStatus.getPath().getName());
//            System.out.println(fileStatus.getBlockSize());
//            System.out.println(fileStatus.getPermission());
//            System.out.println(fileStatus.getLen());
//            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
//            for (BlockLocation bl : blockLocations) {
//                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
//                String[] hosts = bl.getHosts();
//                for (String host : hosts) {
//                    System.out.println(host);
//                }
//            }
//            System.out.println("--------------为angelababy打印的分割线--------------");
//        }
        return listFiles;
    }

    /**
     * 查看文件及文件夹信息
     *
     */
    public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String flag = "d--             ";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile()) flag = "f--         ";
            System.out.println(flag + fstatus.getPath().getName());
        }
    }


}
