package com.ifchange.spark.algorithms.jc.ats.recruitstep.dzx;

import com.alibaba.fastjson.JSONObject;
import com.ifchange.spark.mysql.Mysql;
import com.ifchange.spark.util.MyString;
import com.ifchange.spark.util.ResumeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
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
 * updated_at 19/12/11
 * 输入：      hdfs ats 原始文件
 * 处理逻辑： 根据每一行中的tob_cv_id、jdId 查询数据库拼装成相应的json数据，
 * 拼装在原始文件的每一行的后面
 * 输出：     /basic_data/tob/tob_ats/ats_recruit_step_dzx_v2
 * <p>
 * <p>
 * uid、top_id、resume_id、tob_resume_id、icdc_position_id、step_type、updated_at、cv_json、jd_json
 */
public class FlushRecruitStepForDzxV2 {

    private static final Logger LOG = LoggerFactory.getLogger(FlushRecruitStepForDzxV2.class);

    private static final String POSITION_HOST_1 = "192.168.8.86";

    private static final String POSITION_HOST_2 = "192.168.8.88";

    private static final int POSITION_PORT = 3307;

    private static final int PORT = 3306;

    private static final String USER_NAME = "databus_user";

    private static final String PASS_WORD = "Y,qBpk5vT@zVOwsz";

    private static final String TOB_HOST1 = "192.168.9.135";

    private static final String TOB_HOST2 = "192.168.9.168";

    private static final String TOB_USERNAME = "dataapp_user";

    private static final String TOB_PASSWORD = "Q6pXGo8cP3";

    private static final String TOB_PASSWORD2 = "lz4PWZDK/aMWz";


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

        Dataset<String> results = sparkSession.read().textFile(path).repartition(partition)
            .mapPartitions((MapPartitionsFunction<String, String>) iterator -> {
                Mysql tobMysql135 = new Mysql(TOB_USERNAME, TOB_PASSWORD, "tob_resume_pool_0", TOB_HOST1, PORT);
                Mysql tobMysql168 = new Mysql(TOB_USERNAME, TOB_PASSWORD2, "tob_resume_pool_4", TOB_HOST2, PORT);
                Mysql positionMysql1 = new Mysql(USER_NAME, PASS_WORD, "position_0", POSITION_HOST_1, POSITION_PORT);
                Mysql positionMysql2 = new Mysql(USER_NAME, PASS_WORD, "position_1", POSITION_HOST_2, POSITION_PORT);

                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String str = iterator.next();
                    if (StringUtils.isNoneBlank(str)) {
                        StringBuilder sb = new StringBuilder();
                        String[] split = str.split("\t");
                        if (split.length == 7) {
                            //uid、top_id、resume_id、tob_resume_id、icdc_position_id、step_type、updated_at
                            String uid = split[0];
                            String topId = split[1];
                            String resumeId = split[2];
                            String tobResumeId = split[3];
                            String icdcPositionId = split[4];
                            String stepType = split[5];
                            String updatedAt = split[6];
                            LOG.info("cid:{},jid:{}", tobResumeId, icdcPositionId);
                            sb.append(uid);
                            sb.append("\t");
                            sb.append(topId);
                            sb.append("\t");
                            sb.append(resumeId);
                            sb.append("\t");
                            sb.append(tobResumeId);
                            sb.append("\t");
                            sb.append(icdcPositionId);
                            sb.append("\t");
                            sb.append(stepType);
                            sb.append("\t");
                            sb.append(updatedAt);
                            sb.append("\t");
                            JSONObject cvJson = new JSONObject();
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
                            }
                            sb.append(cvJson);
                            sb.append("\t");

                            JSONObject jdJson = new JSONObject();
                            if (!StringUtils.equals("0", icdcPositionId.trim())) {
                                String positionDbName = ResumeUtil.getPositionDBNameByPid(Long.parseLong(icdcPositionId.trim()));
                                String jdSql = "select p.*,pe.*,pect.*,pa.* from `" + positionDbName + "`.positions p " +
                                    "LEFT JOIN `" + positionDbName + "`.positions_extras pe on p.id=pe.id " +
                                    "LEFT JOIN `" + positionDbName + "`.positions_excepts pect on p.id=pect.id " +
                                    "LEFT JOIN `" + positionDbName + "`.positions_algorithms pa on p.id=pa.id " +
                                    "where p.id=" + icdcPositionId;
                                String positionMapSql = "select * from `" + positionDbName + "`.positions_maps where position_id=" + icdcPositionId;
                                LOG.info(jdSql);
                                LOG.info(positionMapSql);
                                try {
                                    int index = Integer.parseInt(positionDbName.split("_")[1]);
                                    Map<String, Object> result = index % 2 == 0 ? positionMysql1.queryPositions(jdSql)
                                        : positionMysql2.queryPositions(jdSql);
                                    List<Map<String, String>> positionMapList = index % 2 == 0 ? positionMysql1.queryPositionMaps(positionMapSql)
                                        : positionMysql2.queryPositionMaps(positionMapSql);
                                    result.put("map", positionMapList);
                                    jdJson.put(icdcPositionId, result);
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
                tobMysql135.close();
                tobMysql168.close();
                positionMysql1.close();
                positionMysql2.close();
                return list.iterator();
            }, Encoders.STRING());

        //save text
        results.write().text(savePath);

    }
}
