package com.ifchange.spark.core.hdfs;

import com.ifchange.spark.util.DateTimeUtil;
import com.ifchange.spark.util.HDFSUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHdfs {

    private static final Logger LOG = LoggerFactory.getLogger(TestHdfs.class);

    public static void main(String[] args) {
        try {
            RemoteIterator<LocatedFileStatus> listFiles = HDFSUtil.testListFiles(args[0]);
            while (listFiles.hasNext()) {
                LocatedFileStatus file = listFiles.next();
                LOG.info("file name:{}\tPermission:{}\tOwner:{}\tGroup:{}\tSize:{}\tLast Modified:{}\t{}",
                    file.getPath().getName(),
                    file.getPermission().toString(),
                    file.getOwner(),
                    file.getGroup(),
                    file.getLen(),
                    file.getModificationTime(),
                    DateTimeUtil.formatLongTimeToStandard(file.getModificationTime()));

                /**
                 * blockLocations的长度是几？  是什么意义？
                 * 块的数量
                 */
//            BlockLocation[] blockLocations = file.getBlockLocations();
//            System.out.println(blockLocations.length + "\t");
//            for (BlockLocation bl : blockLocations) {
//                String[] hosts = bl.getHosts();
//                System.out.print(hosts[0] + "-" + hosts[1] + "\t");
//            }
//            System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
