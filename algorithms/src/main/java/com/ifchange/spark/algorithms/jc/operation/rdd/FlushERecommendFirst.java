package com.ifchange.spark.algorithms.jc.operation.rdd;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 *  e_recommend 第一次刷全量
 *  输入：      hdfs e_recommend 原始文件
 *  处理逻辑： 根据每一行中的cvId、jdId 查询数据库拼装成相应的json数据，
 *             拼装在原始文件的每一行的后面
 *  输出：     hdfs e_recommend_all_$date
 */
public class FlushERecommendFirst {

    private static final Logger logger = LoggerFactory.getLogger(FlushERecommendFirst.class);

    private static final String ICDC_HOST_1 = "192.168.8.134";

    private static final String ICDC_HOST_2 = "192.168.8.136";

    private static final int ICDC_PORT = 3306;

    private static final String POSITION_HOST_1 = "192.168.8.86";

    private static final String POSITION_HOST_2 = "192.168.8.88";

    private static final int POSITION_PORT = 3307;

    private static final String USER_NAME = "databus_user";

    private static final String PASS_WORD = "Y,qBpk5vT@zVOwsz";

    private static final SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {

        if (args.length < 4) {
            logger.info("args length is not correct");
            System.exit(-1);
        }

        String appName = args[0];
        String master = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> resultRdd = jsc.textFile(path, partition)
                .filter(StringUtils::isNoneBlank)
                .mapPartitions((FlatMapFunction<Iterator<String>, String>) stringIterator -> {
                    ArrayList<String> list = new ArrayList<>();
                    Mysql icdcMysql1 = null;
                    Mysql icdcMysql2 = null;
                    Mysql positionMysql1 = null;
                    Mysql positionMysql2 = null;
                    try {
                        icdcMysql1 = new Mysql(USER_NAME, PASS_WORD, "icdc_0", ICDC_HOST_1, ICDC_PORT);
                        icdcMysql2 = new Mysql(USER_NAME, PASS_WORD, "icdc_1", ICDC_HOST_2, ICDC_PORT);
                        positionMysql1 = new Mysql(USER_NAME, PASS_WORD, "position_0", POSITION_HOST_1, POSITION_PORT);
                        positionMysql2 = new Mysql(USER_NAME, PASS_WORD, "position_1", POSITION_HOST_2, POSITION_PORT);
                    } catch (Exception e) {
                        logger.info("init mysql error:{}", e.getMessage());
                    }
                    if (null != icdcMysql1 && null != icdcMysql2 && null != positionMysql1 && null != positionMysql2) {
                        while (stringIterator.hasNext()) {
                            StringBuilder sb = new StringBuilder();
                            String str = stringIterator.next();
                            String[] split = str.split("\t");
                            String cid = split[6];
                            String jid = split[5];
                            logger.info("cid:{},jid:{}", cid, jid);
                            sb.append(str);
                            sb.append("\t");
                            JSONObject cvJson = new JSONObject();
                            //组装cvJSon
                            String icdcDbName = ResumeUtil.getDBNameById2(Long.parseLong(cid.trim()));
                            String cvSql = "select rs.compress,column_json(a.data) as algorithms_data from `" + icdcDbName + "`.resumes_extras rs " +
                                    " LEFT JOIN `" + icdcDbName + "`.algorithms a on rs.id=a.id where rs.id=" + cid;
                            String mapSql = "select * from `" + icdcDbName + "`.resumes_maps rm where rm.resume_id=" + cid;
                            logger.info(cvSql);
                            logger.info(mapSql);
                            try {
                                if (Integer.parseInt(icdcDbName.split("_")[1]) % 2 == 0) {
                                    String s = icdcMysql1.queryCompressAndAlgorithms(cvSql);
                                    if (StringUtils.isNoneBlank(s)) {
                                        JSONObject jsonObject = JSONObject.parseObject(s);
                                        List<Map<String, String>> resumeMapsList = icdcMysql1.queryResumesMaps(mapSql);
                                        jsonObject.put("map", resumeMapsList);
                                        cvJson.put(cid, jsonObject);
                                    }
                                } else {
                                    String s = icdcMysql2.queryCompressAndAlgorithms(cvSql);
                                    if (StringUtils.isNoneBlank(s)) {
                                        JSONObject jsonObject = JSONObject.parseObject(s);
                                        List<Map<String, String>> resumeMapsList = icdcMysql2.queryResumesMaps(mapSql);
                                        jsonObject.put("map", resumeMapsList);
                                        cvJson.put(cid, jsonObject);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                logger.info("query icdc sql error:{}", e.getMessage());
                            }
                            sb.append(cvJson);
                            sb.append("\t");
                            JSONObject jdJson = new JSONObject();
                            if (!jid.trim().equals("0")) {
                                String positionDbName = ResumeUtil.getPositionDBNameByPid(Long.parseLong(jid));
                                String jdSql = "select p.*,pe.*,pect.*,pa.* from `" + positionDbName + "`.positions p " +
                                        "LEFT JOIN `" + positionDbName + "`.positions_extras pe on p.id=pe.id " +
                                        "LEFT JOIN `" + positionDbName + "`.positions_excepts pect on p.id=pect.id " +
                                        "LEFT JOIN `" + positionDbName + "`.positions_algorithms pa on p.id=pa.id " +
                                        "where p.id=" + jid;
                                String positionMapSql = "select * from `" + positionDbName + "`.positions_maps where position_id=" + jid;
                                logger.info(jdSql);
                                logger.info(positionMapSql);
                                try {
                                    if (Integer.parseInt(positionDbName.split("_")[1]) % 2 == 0) {
                                        Map<String, Object> result = positionMysql1.queryPositions(jdSql);
                                        List<Map<String, String>> positionMapList = positionMysql1.queryPositionMaps(positionMapSql);
                                        result.put("map", positionMapList);
                                        jdJson.put(jid, result);
                                    } else {
                                        Map<String, Object> result = positionMysql2.queryPositions(jdSql);
                                        List<Map<String, String>> positionMapList = positionMysql2.queryPositionMaps(positionMapSql);
                                        result.put("map", positionMapList);
                                        jdJson.put(jid, result);
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.info("query position sql error:{}", e.getMessage());
                                }
                            }
                            sb.append(jdJson);
                            list.add(sb.toString());
                        }
                        icdcMysql1.close();
                        icdcMysql2.close();
                        positionMysql1.close();
                        positionMysql2.close();
                    }
                    return list.iterator();
                });

        resultRdd.saveAsTextFile("/basic_data/tob/operation/e_recommend_all_"+sdf.format(new Date()));
    }
}
