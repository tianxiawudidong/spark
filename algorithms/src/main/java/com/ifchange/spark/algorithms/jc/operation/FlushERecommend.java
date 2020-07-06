package com.ifchange.spark.algorithms.jc.operation;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * updated_at 19/9/12
 */
public class FlushERecommend {

    private static final Logger LOG = LoggerFactory.getLogger(FlushERecommend.class);

    private static final String ICDC_HOST_1 = "192.168.8.134";

    private static final String ICDC_HOST_2 = "192.168.8.136";

    private static final int ICDC_PORT = 3306;

    private static final String POSITION_HOST_1 = "192.168.8.86";

    private static final String POSITION_HOST_2 = "192.168.8.88";

    private static final int POSITION_PORT = 3307;

    private static final String USER_NAME = "databus_user";

    private static final String PASS_WORD = "Y,qBpk5vT@zVOwsz";

    public static void main(String[] args) {

        if (args.length < 5) {
            LOG.info("args length is not correct");
            System.exit(-1);
        }

        String appName = args[0];
        String master = args[1];
        int partition = Integer.parseInt(args[2]);
        String path = args[3];
        String savePath = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<String> result = spark.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) stringIterator -> {
                List<String> list = new ArrayList<>();

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
                    LOG.info("init mysql error:{}", e.getMessage());
                }

                if (null != icdcMysql1 && null != icdcMysql2 && null != positionMysql1 && null != positionMysql2) {
                    while (stringIterator.hasNext()) {
                        StringBuilder sb = new StringBuilder();
                        String str = stringIterator.next();
                        if (StringUtils.isNoneBlank(str)) {
                            String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\t");
                            if (split.length == 19) {
                                String jid = split[5];
                                String cid = split[6];
                                String format = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
                                    split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8],
                                    split[9], split[10], split[11], split[12], split[13], split[14], split[15], split[16]
                                );
                                LOG.info("cid:{},jid:{}", cid, jid);
                                sb.append(format);
                                sb.append("\t");
                                JSONObject cvJson = new JSONObject();
                                //组装cvJSon
                                String icdcDbName = ResumeUtil.getDBNameById2(Long.parseLong(cid.trim()));
                                String cvSql = "select rs.compress,column_json(a.data) as algorithms_data from `" + icdcDbName + "`.resumes_extras rs " +
                                    " LEFT JOIN `" + icdcDbName + "`.algorithms a on rs.id=a.id where rs.id=" + cid;
                                String mapSql = "select * from `" + icdcDbName + "`.resumes_maps rm where rm.resume_id=" + cid;
                                LOG.info(cvSql);
                                LOG.info(mapSql);
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
                                    LOG.info("query icdc sql error:{}", e.getMessage());
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
                                    LOG.info(jdSql);
                                    LOG.info(positionMapSql);
                                    try {
                                        if (Integer.parseInt(positionDbName.split("_")[1]) % 2 == 0) {
                                            Map<String, Object> map = positionMysql1.queryPositions(jdSql);
                                            List<Map<String, String>> positionMapList = positionMysql1.queryPositionMaps(positionMapSql);
                                            map.put("map", positionMapList);
                                            jdJson.put(jid, map);
                                        } else {
                                            Map<String, Object> map = positionMysql2.queryPositions(jdSql);
                                            List<Map<String, String>> positionMapList = positionMysql2.queryPositionMaps(positionMapSql);
                                            map.put("map", positionMapList);
                                            jdJson.put(jid, map);
                                        }
                                    } catch (Exception e) {
                                        LOG.info("query position sql error:{}", e.getMessage());
                                    }
                                }
                                sb.append(jdJson);
                            } else {
                                sb.append(str);
                            }
                        }
                        list.add(sb.toString());
                    }
                    icdcMysql1.close();
                    icdcMysql2.close();
                    positionMysql1.close();
                    positionMysql2.close();
                }
                return list.iterator();
            }, Encoders.STRING());

        result.write().save(savePath);
    }

}
