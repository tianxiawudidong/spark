package com.ifchange.spark.algorithms.jc.ats.recruitstep.dzx;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * updated_at 19/11/14
 * 输入：      hdfs ats 原始文件
 * /user/hive/warehouse/tob_ats_v2.db/ats_merge_aon
 * /user/hive/warehouse/tob_ats_v2.db/ats_merge_greentown
 * 处理逻辑： 根据每一行中的cvId、jdId 查询数据库拼装成相应的json数据，
 * 拼装在原始文件的每一行的后面
 * 输出：     /basic_data/tob/tob_ats/ats_merge_aon
 * /basic_data/tob/tob_ats/ats_merge_greentown
 * <p>
 * <p>
 * uid、icdc_position_id、 resume_id、 step_type
 */
public class FlushRecruitStepForDZX {

    private static final Logger LOG = LoggerFactory.getLogger(FlushRecruitStepForDZX.class);

    private static final String ICDC_HOST_1 = "192.168.8.134";

    private static final String ICDC_HOST_2 = "192.168.8.136";

    private static final int ICDC_PORT = 3306;

    private static final String POSITION_HOST_1 = "192.168.8.86";

    private static final String POSITION_HOST_2 = "192.168.8.88";

    private static final int POSITION_PORT = 3307;

    private static final String USER_NAME = "databus_user";

    private static final String PASS_WORD = "Y,qBpk5vT@zVOwsz";


    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String path = args[2];
        int partition = Integer.parseInt(args[3]);
        String savePath = args[4];

        SparkConf conf = new SparkConf();
        conf.setMaster(master);
        conf.setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Dataset<String> results = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                Mysql icdcMysql1 = new Mysql(USER_NAME, PASS_WORD, "icdc_0", ICDC_HOST_1, ICDC_PORT);
                Mysql icdcMysql2 = new Mysql(USER_NAME, PASS_WORD, "icdc_1", ICDC_HOST_2, ICDC_PORT);
                Mysql positionMysql1 = new Mysql(USER_NAME, PASS_WORD, "position_0", POSITION_HOST_1, POSITION_PORT);
                Mysql positionMysql2 = new Mysql(USER_NAME, PASS_WORD, "position_1", POSITION_HOST_2, POSITION_PORT);

                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String str = iterator.next();
                    if (StringUtils.isNoneBlank(str)) {
                        StringBuilder sb = new StringBuilder();
                        String[] split = str.split("\t");
                        if (split.length == 4) {
                            //uid、icdc_position_id、 resume_id、 step_type
                            String uid = split[0];
                            String jid = split[1];
                            String cid = split[2];
                            String step_type = split[3];
                            LOG.info("cid:{},jid:{}", cid, jid);
                            sb.append(uid);
                            sb.append("\t");
                            sb.append(jid);
                            sb.append("\t");
                            sb.append(cid);
                            sb.append("\t");
                            sb.append(step_type);
                            sb.append("\t");
                            JSONObject cvJson = new JSONObject();
                            if (!StringUtils.equals("0", cid.trim())) {
                                //组装cvJSon
                                String icdcDbName = ResumeUtil.getDBNameById2(Long.parseLong(cid.trim()));
                                String cvSql = "select rs.compress,column_json(a.data) as algorithms_data from `" + icdcDbName + "`.resumes_extras rs " +
                                    " LEFT JOIN `" + icdcDbName + "`.algorithms a on rs.id=a.id where rs.id=" + cid;
                                String mapSql = "select * from `" + icdcDbName + "`.resumes_maps rm where rm.resume_id=" + cid;
                                LOG.info(cvSql);
                                LOG.info(mapSql);
                                try {
                                    int index = Integer.parseInt(icdcDbName.split("_")[1]);
                                    String s = index % 2 == 0 ? icdcMysql1.queryCompressAndAlgorithms(cvSql)
                                        : icdcMysql2.queryCompressAndAlgorithms(cvSql);
                                    if (StringUtils.isNoneBlank(s)) {
                                        JSONObject jsonObject = JSONObject.parseObject(s);
                                        List<Map<String, String>> resumeMapsList = index % 2 == 0
                                            ? icdcMysql1.queryResumesMaps(mapSql) : icdcMysql2.queryResumesMaps(mapSql);
                                        jsonObject.put("map", resumeMapsList);
                                        cvJson.put(cid, jsonObject);
                                    }
                                } catch (Exception e) {
                                    LOG.info("query icdc sql error:{}", e.getMessage());
                                    e.printStackTrace();
                                }
                            }
                            sb.append(cvJson);
                            sb.append("\t");
                            JSONObject jdJson = new JSONObject();
                            if (!StringUtils.equals("0", jid.trim())) {
                                String positionDbName = ResumeUtil.getPositionDBNameByPid(Long.parseLong(jid.trim()));
                                String jdSql = "select p.*,pe.*,pect.*,pa.* from `" + positionDbName + "`.positions p " +
                                    "LEFT JOIN `" + positionDbName + "`.positions_extras pe on p.id=pe.id " +
                                    "LEFT JOIN `" + positionDbName + "`.positions_excepts pect on p.id=pect.id " +
                                    "LEFT JOIN `" + positionDbName + "`.positions_algorithms pa on p.id=pa.id " +
                                    "where p.id=" + jid;
                                String positionMapSql = "select * from `" + positionDbName + "`.positions_maps where position_id=" + jid;
                                LOG.info(jdSql);
                                LOG.info(positionMapSql);
                                try {
                                    int index = Integer.parseInt(positionDbName.split("_")[1]);
                                    Map<String, Object> result = index % 2 == 0 ? positionMysql1.queryPositions(jdSql)
                                        : positionMysql2.queryPositions(jdSql);
                                    List<Map<String, String>> positionMapList = index % 2 == 0 ? positionMysql1.queryPositionMaps(positionMapSql)
                                        : positionMysql2.queryPositionMaps(positionMapSql);
                                    result.put("map", positionMapList);
                                    jdJson.put(jid, result);
                                } catch (Exception e) {
                                    LOG.info("query position sql error:{}", e.getMessage());
                                    e.printStackTrace();
                                }
                            }
                            sb.append(jdJson);
                        } else {
                            LOG.info("length is not 6,is:{}", split.length);
                        }
                        list.add(sb.toString());
                    }
                }
                icdcMysql1.close();
                icdcMysql2.close();
                positionMysql1.close();
                positionMysql2.close();
                return list.iterator();
            }, Encoders.STRING()).map((MapFunction<String, String>) s -> s, Encoders.STRING());

        //save text
        results.write().text(savePath);

    }
}
