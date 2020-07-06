package com.ifchange.spark.algorithms.jc.ats.v3;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.MyString;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * ats recruit_step 第一次刷全量
 * 输入：      hdfs ats 原始文件
 * 处理逻辑： 根据每一行中的cvId、jdId 查询数据库拼装成相应的json数据，
 * 拼装在原始文件的每一行的后面
 * 输出：
 * <p>
 * <p>
 * a.uid, b.icdc_position_id, a.resume_id,a.tob_resume_id, a.stage_type_id, a.stage_type_from, a.updated_at,cv_json,jd_json
 */
public class FlushRecruitStepV3 {


    private static final Logger LOG = LoggerFactory.getLogger(FlushRecruitStepV3.class);

    private static final String ICDC_HOST_1 = "192.168.8.134";

    private static final String ICDC_HOST_2 = "192.168.8.136";

    private static final int ICDC_PORT = 3306;

    private static final String POSITION_HOST_1 = "192.168.8.86";

    private static final String POSITION_HOST_2 = "192.168.8.88";

    private static final int POSITION_PORT = 3307;

    private static final String TOB_HOST1 = "192.168.9.135";

    private static final String TOB_HOST2 = "192.168.9.168";

    private static final String TOB_USERNAME = "dataapp_user";

    private static final String TOB_PASSWORD = "Q6pXGo8cP3";

    private static final String TOB_PASSWORD2 = "lz4PWZDK/aMWz";

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
        LongAccumulator tobAccumulator = sparkSession.sparkContext().longAccumulator("tob_accumulator");
        LongAccumulator tobNoJsonAccumulator = sparkSession.sparkContext().longAccumulator("tob_no_json_accumulator");
        LongAccumulator icdcAccumulator = sparkSession.sparkContext().longAccumulator("icdc_accumulator");

        Dataset<String> results = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                Mysql icdcMysql1 = new Mysql(USER_NAME, PASS_WORD, "icdc_0", ICDC_HOST_1, ICDC_PORT);
                Mysql icdcMysql2 = new Mysql(USER_NAME, PASS_WORD, "icdc_1", ICDC_HOST_2, ICDC_PORT);
                Mysql positionMysql1 = new Mysql(USER_NAME, PASS_WORD, "position_0", POSITION_HOST_1, POSITION_PORT);
                Mysql positionMysql2 = new Mysql(USER_NAME, PASS_WORD, "position_1", POSITION_HOST_2, POSITION_PORT);
                Mysql tobMysql135 = new Mysql(TOB_USERNAME, TOB_PASSWORD, "tob_resume_pool_0", TOB_HOST1, ICDC_PORT);
                Mysql tobMysql168 = new Mysql(TOB_USERNAME, TOB_PASSWORD2, "tob_resume_pool_4", TOB_HOST2, ICDC_PORT);

                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String str = iterator.next();
                    if (StringUtils.isNoneBlank(str)) {
                        StringBuilder sb = new StringBuilder();
                        String[] split = str.split("\t");
                        if (split.length == 7) {
                            //a.uid, b.icdc_position_id, a.resume_id,a.tob_resume_id, a.stage_type_id, a.stage_type_from, a.updated_at
                            String uid = split[0];
                            String jid = split[1];
                            String icdcResumeId = split[2];
                            String tobResumeId = split[3];
                            String stageTypeId = split[4];
                            String stageTypeFrom = split[5];
                            String updatedAt = split[6];
                            LOG.info("icdc_id:{},tob_id:{},jid:{}", icdcResumeId, tobResumeId, jid);
                            sb.append(uid);
                            sb.append("\t");
                            sb.append(jid);
                            sb.append("\t");
                            sb.append(icdcResumeId);
                            sb.append("\t");
                            sb.append(tobResumeId);
                            sb.append("\t");
                            sb.append(stageTypeId);
                            sb.append("\t");
                            sb.append(stageTypeFrom);
                            sb.append("\t");
                            sb.append(updatedAt);
                            sb.append("\t");
                            JSONObject cvJson = new JSONObject();
                            //先从tob库取相关json，如果没有则取icdc库
                            if (!StringUtils.equalsIgnoreCase(tobResumeId, "0")) {
                                Map<String, String> result = null;
                                try {
                                    String tobAlgorithmsDBName = ResumeUtil.getTobAlgorithmsDBName(tobResumeId);
                                    String algorithmsTableName = ResumeUtil.getTobAlgorithmsTableName(tobResumeId);
                                    String tobDBName = ResumeUtil.getTobDBName(tobResumeId);
                                    String detailTableName = ResumeUtil.getTobDetailTableName(tobResumeId);
                                    int index = Integer.parseInt(StringUtils.splitByWholeSeparatorPreserveAllTokens(tobAlgorithmsDBName, "_")[3]);
                                    LOG.info("tob_id:{} index:{}", tobResumeId, index);
                                    String tobSql = "select a.resume_content,column_json(b.data) as data" +
                                        " from `" + tobDBName + "`." + detailTableName + " a " +
                                        " left join `" + tobAlgorithmsDBName + "`." + algorithmsTableName + " b " +
                                        " on a.tob_resume_id=b.tob_resume_id " +
                                        " where a.tob_resume_id=" + tobResumeId;
                                    LOG.info(tobSql);
                                    result = index < 4 ? tobMysql135.getTobCvJSON(tobSql)
                                        : tobMysql168.getTobCvJSON(tobSql);
                                } catch (Exception e) {
                                    LOG.error("tob_id:{} query error:{}", tobResumeId, e.getMessage());
                                }
                                if (null != result && result.size() > 0) {
                                    tobAccumulator.add(1L);
                                    String resumeContent = result.get("resume_content");
                                    String algorithmsData = result.get("data");
                                    JSONObject jsonObject = new JSONObject();
                                    String content;
                                    if (StringUtils.isNoneBlank(resumeContent)) {
                                        content = new String(MyString.gzipUncompress(Base64.getDecoder().decode(resumeContent)));
                                        content = content.replaceAll("\r|\n", "");
                                        try {
                                            jsonObject = JSONObject.parseObject(content);
                                        } catch (Exception e) {
                                            LOG.error("tob_id:{} content is not json", tobResumeId);
                                            tobNoJsonAccumulator.add(1L);
//                                            jsonObject.put("compress", content);
                                        }
                                    }
                                    if (jsonObject.size() > 0) {
                                        JSONObject resultJson = new JSONObject();
                                        if (StringUtils.isNoneBlank(algorithmsData)) {
                                            JSONObject dataJson = JSONObject.parseObject(algorithmsData);
                                            if (null != dataJson && dataJson.size() > 0) {
                                                for (Map.Entry<String, Object> entry : dataJson.entrySet()) {
                                                    String key = entry.getKey();
                                                    String data = "";
                                                    String values =
                                                        null != entry.getValue() ? String.valueOf(entry.getValue())
                                                            : "";
                                                    try {
                                                        byte[] decode = Base64.getDecoder().decode(values);
                                                        if (null != decode) {
                                                            byte[] bytes = MyString.gzipUncompress(decode);
                                                            if (null != bytes) {
                                                                values = new String(bytes);
                                                            }
                                                        }
                                                    } catch (Exception e) {
                                                        LOG.error("id:{} gzip uncompress error:{}", tobResumeId, e.getMessage());
                                                    }
                                                    if (StringUtils.isNoneBlank(values)) {
                                                        data = MyString.decodeUnicode(values);
                                                    }
                                                    resultJson.put(key, data);
                                                }
                                            }
                                        }
                                        jsonObject.put("algorithm", resultJson);
                                        cvJson.put(tobResumeId, jsonObject);
                                    }
                                }
                            } else {
                                if (!StringUtils.equals("0", tobResumeId.trim())) {
                                    icdcAccumulator.add(1L);
                                    //组装cvJSon
                                    String icdcDbName = ResumeUtil.getDBNameById2(Long.parseLong(icdcResumeId.trim()));
                                    String cvSql = "select rs.compress,column_json(a.data) as algorithms_data " +
                                        " from `" + icdcDbName + "`.resumes_extras rs " +
                                        " left join `" + icdcDbName + "`.algorithms a on rs.id=a.id where rs.id=" + icdcResumeId;
                                    String mapSql = "select * from `" + icdcDbName + "`.resumes_maps rm where rm.resume_id=" + icdcResumeId;
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
                                            cvJson.put(icdcResumeId, jsonObject);
                                        }
                                    } catch (Exception e) {
                                        LOG.error("query icdc sql error:{}", e.getMessage());
                                        e.printStackTrace();
                                    }
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
                            LOG.info("length is not 7,is:{}", split.length);
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
