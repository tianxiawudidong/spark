package com.ifchange.spark.streaming.util;

import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.*;

public class MyKafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaUtil.class);

    public static void readOffsets(String topic, String groupId, ZkClient zkClient) {
        Map<TopicPartition, Long> topicPartOffsetMap = new HashMap<>();
        String path=String.format("/brokers/topics/%s/partitions/0/state",topic);
        System.out.println(path);
        Object o = zkClient.readData(path);
        System.out.println(o.toString());
    }


//    def readOffsets(topics: Seq[String], groupId: String, zkUtils: ZkUtils): Map[TopicPartition, Long] = {
//
//        val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
//        val partitionMap = zkUtils.getPartitionsForTopics(topics)
//        // /consumers/<groupId>/offsets/<topic>/
//        partitionMap.foreach(topicPartitions => {
//            val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
//            topicPartitions._2.foreach(partition => {
//                val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition
//        try {
//            val offsetStatTuple = zkUtils.readData(offsetPath)
//            if (offsetStatTuple != null) {
//                LOGGER.info("retrieving offset details - topic: {}, partition: {}, offset: {}, node path: {}",
//                    topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath)
//                topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)),
//                    offsetStatTuple._1.toLong)
//            }
//        } catch {
//            case e: Exception =>
//                LOGGER.warn("retrieving offset details - no previous node exists:" + " {}, topic: {}, partition: {}, node path: {}",
//                    e.getMessage, topicPartitions._1, partition.toString, offsetPath)
////            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
//        }
//      })
//    })
//        topicPartOffsetMap.toMap
//    }


    public static void main(String[] args) {
        System.out.println(11111);
        ZkClient zkClient = new ZkClient("192.168.1.200:2181");
        readOffsets("resume_algorithm","aa",zkClient);

    }
}
